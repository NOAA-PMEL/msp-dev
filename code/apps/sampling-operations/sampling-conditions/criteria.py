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
        return False

class LimitMinMax(SamplingCriterion):
    """
    Docstring for LimitMinMax
    """

    def __init__(self, config):
        super(LimitMinMax, self).__init__(config)
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
        result = []
        for source_var in self.get_sources():
            if source_var in sources:
                source_val = sources[source_val]
                if self.true_if == "inside":
                    result.append(source_val >= self.min_val and source_val <= self.max_val)
                elif self.true_if == "outside":
                    result.append(source_val < self.min_val and source_val > self.max_val)
        return all(result)


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


