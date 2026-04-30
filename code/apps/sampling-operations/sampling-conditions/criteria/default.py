import asyncio
import importlib
import json
import logging
import math
from time import sleep

# import numpy as np
# from ulid import ULID
# from pathlib import Path
import os

# import httpx
from logfmter import Logfmter

from pydantic import BaseModel, BaseSettings

# from sampling_conditions import SamplingCriterion
# from cloudevents.http import CloudEvent, from_http

# # from cloudevents.http.conversion import from_http
# from cloudevents.conversion import to_structured  # , from_http
# from cloudevents.exceptions import InvalidStructuredJSON

from datetime import datetime, timezone

class SamplingCriterion:
    """
    Docstring for SamplingCriterion
    """

    def __init__(self, config):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("SamplingCriterion instantiated")

        self.config = config
        self.source_names = []
        self.configure()

    def configure(self):

        if "sources" in self.config:
            self.source_names = [n for n in self.config["sources"]]

    def get_sources(self):
        return self.source_names

    async def evaluate(self, sources) -> bool:
        self.debug("evaluate-base")
        return False

class LimitMinMax(SamplingCriterion):
    """
    Docstring for LimitMinMax
    """

    def __init__(self, config):
        super(LimitMinMax, self).__init__(config)
        self.logger.debug("LimitMinMax instantiated")
        self.min_value = None
        self.max_value = None
        self.true_if = "inside"  # inside or outside of value >=min and <=max

    def configure(self):
        super(LimitMinMax, self).configure()

        if "max_val" in self.config:
            self.max_val = self.config["max_val"]
        if "min_val" in self.config:
            self.min_val = self.config["min_val"]
        if "true_if" in self.config:
            if (val := self.config["true_if"]) in ["inside", "outside"]:
                self.true_if = val

    async def evaluate(self, sources) -> bool:
        super(LimitMinMax, self).evaluate(sources)
        result = []
        self.logger.debug("evaluate", extra={"src_vals": sources})
        for source_var in self.get_sources():
            if source_var in sources:
                source_val = sources[source_var]
                if self.true_if == "inside":
                    self.logger.debug("evaluate", extra={"src_val": source_val, "min_val": self.min_val, "max_val": self.max_val})
                    result.append(source_val >= self.min_val and source_val <= self.max_val)
                elif self.true_if == "outside":
                    result.append(source_val < self.min_val or source_val > self.max_val)
                self.logger.debug("evaluate", extra={"eval_result": result, "return_result": all(result)})
        return all(result)

class LatLonRegionLocation(SamplingCriterion):
    """
    Evaluates if a current (lat, lon) position is inside or outside a defined geographic region.
    - 2 points: Evaluates as a Bounding Box (South-West to North-East).
    - 3+ points: Evaluates as a Polygon using Ray-Casting.
    Supports regions spanning the International Date Line.
    """
    def __init__(self, config):
        super(LatLonRegionLocation, self).__init__(config)
        self.label = config.get("label", "Unknown Region")
        self.region = config.get("region", [])
        self.coords_map = config.get("coordinates", {})
        self.true_if = config.get("true_if", "inside")

    def _normalize_longitude(self, lon, wrap_offset=0):
        """Helper to shift longitudes to 0-360 if the polygon crosses the IDL."""
        shifted_lon = lon + wrap_offset
        if shifted_lon < 0 and wrap_offset > 0:
            shifted_lon += 360
        return shifted_lon

    def _point_in_polygon(self, x, y, polygon):
        """
        Ray-casting algorithm for Point in Polygon.
        x = longitude, y = latitude
        """
        n = len(polygon)
        inside = False
        p1x, p1y = polygon[0]
        for i in range(1, n + 1):
            p2x, p2y = polygon[i % n]
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y
        return inside

    async def evaluate(self, sources) -> bool:
        super(LatLonRegionLocation, self).evaluate(sources)
        
        lat_src = self.coords_map.get("latitude")
        lon_src = self.coords_map.get("longitude")

        if lat_src not in sources or lon_src not in sources:
            self.logger.warning(f"Missing coordinate sources for {self.label}")
            return False

        if len(self.region) < 2:
            self.logger.warning(f"Region {self.label} must have at least 2 points defined.")
            return False

        curr_lat = sources[lat_src]
        curr_lon = sources[lon_src]

        # ---------------------------------------------------------
        # Case 1: Simple Bounding Box (2 Points)
        # ---------------------------------------------------------
        if len(self.region) == 2:
            lats = [p["latitude"] for p in self.region]
            lons = [p["longitude"] for p in self.region]
            
            min_lat, max_lat = min(lats), max(lats)
            min_lon, max_lon = min(lons), max(lons)

            in_lat = min_lat <= curr_lat <= max_lat

            # IDL Wrap-Around detection
            if abs(max_lon - min_lon) > 180:
                in_lon = curr_lon >= max_lon or curr_lon <= min_lon
            else:
                in_lon = min_lon <= curr_lon <= max_lon
                
            is_inside = in_lat and in_lon

        # ---------------------------------------------------------
        # Case 2: Complex Polygon (3+ Points)
        # ---------------------------------------------------------
        else:
            lons = [p["longitude"] for p in self.region]
            crossing_idl = (max(lons) - min(lons)) > 180
            wrap_offset = 360 if crossing_idl else 0

            polygon_points = []
            for p in self.region:
                norm_lon = self._normalize_longitude(p["longitude"], wrap_offset)
                polygon_points.append((norm_lon, p["latitude"]))

            norm_curr_lon = self._normalize_longitude(curr_lon, wrap_offset)
            is_inside = self._point_in_polygon(norm_curr_lon, curr_lat, polygon_points)
        
        # ---------------------------------------------------------
        # Apply True/False logic and Return
        # ---------------------------------------------------------
        result = is_inside if self.true_if == "inside" else not is_inside
        
        self.logger.debug(
            "evaluate LatLonRegionLocation", 
            extra={
                "label": self.label,
                "lat": curr_lat, 
                "lon": curr_lon, 
                "is_inside": is_inside, 
                "result": result,
                "mode": "bounding_box" if len(self.region) == 2 else "polygon"
            }
        )
        
        return result

async def eval_min_max_limit(criterion: dict, source_data: dict):
    crit_source = criterion["source"]
    data = source_data["crit_source"]["data"]
    for val in data:
        # if not in valid range for any value in data
        if val < criterion["source"]["min_val"] or val > criterion["source"]["min_val"]:
            return False
    
    # all values satisfy the criterion
    return True

async def eval_lat_lon_region(criterion: dict, source_data: dict):
    
    pass

async def eval_compare_mean(criterion: dict, source_data: dict):
    
    pass


eval_fn_map = {
    "MinMaxLimit": eval_min_max_limit,
    "LatLonRegionLocation": eval_lat_lon_region,
    "CompareMean": eval_compare_mean
}

async def evaluate_criteria(criteria: dict, sources:dict, evaluation_time:int=1)->bool:

    # get source data for eval time
    source_data = dict()
    for source_name, source in sources.items():
        # get variableset::variable data for eval_time from datastore
        data = set_
        data=[]
        source_data[source_name] = {"data": data}

    results = []
    for criteria_group, criteria_list in criteria.items():
        group_results = []
        for criterion in criteria_list:
            if criterion["kind"] in eval_fn_map:
                group_results.append(eval_fn_map[criterion["kind"]](criterion=criterion, source_data=source_data, evaluation_time=evaluation_time))
        
        if criteria_group == "all":
            results.append(all(group_results))
        elif criteria_group == "any":
            results.append(any(group_results))
        elif criteria_group == "none":
            results.append(not all(group_results))

    return all(results)


