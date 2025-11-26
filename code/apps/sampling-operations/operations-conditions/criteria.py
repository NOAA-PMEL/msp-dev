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
# from cloudevents.http import CloudEvent, from_http

# # from cloudevents.http.conversion import from_http
# from cloudevents.conversion import to_structured  # , from_http
# from cloudevents.exceptions import InvalidStructuredJSON

from datetime import datetime, timezone


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


