import httpx
import logging
import time
from cachetools import cached, TTLCache
from pydantic import BaseSettings

L = logging.getLogger(__name__)

class Settings(BaseSettings):
    daq_id: str = "mspbase01"
    external_hostname: str = "mspbase01.pmel.noaa.gov"
    ws_port: str = "8080"
    ws_use_tls: str = "false"
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()

# FIX: Remove port 8080. Let it hit the standard cluster port 80 handled by the K8s Service
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

# Shared memory cache: up to 128 unique endpoints, stored for 5 minutes (300s)
registry_cache = TTLCache(maxsize=128, ttl=300)

@cached(cache=registry_cache)
def get_registry_data(endpoint_or_resource: str):
    """
    Safely fetches data using httpx. Automatically translates shorthand 
    resource strings to unified registry API paths.
    """
    # FIX: Smart URL Builder translation
    if "/" not in endpoint_or_resource:
        url = f"http://{datastore_url}/{endpoint_or_resource}-definition/registry/get/"
    else:
        url = f"http://{datastore_url}/{endpoint_or_resource}"
    
    L.info(f"[TIMING] Cache miss! Initiating fetch to {url}")
    start_time = time.time()
    
    try:
        # 5 second timeout is plenty for internal cluster traffic
        with httpx.Client(timeout=5.0) as client:
            L.info(f"[TIMING] HTTPX Client opened, sending GET request...")
            response = client.get(url)
            
            elapsed = time.time() - start_time
            L.info(f"[TIMING] Response received in {elapsed:.3f} seconds. Status: {response.status_code}")
            
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            L.info(f"[TIMING] Successfully parsed {len(results)} registry items.")
            return results
        else:
            L.warning(f"[TIMING] API returned non-200 code: {response.status_code}. Body: {response.text}")
            
    except httpx.ReadTimeout:
        elapsed = time.time() - start_time
        L.error(f"[TIMING] TIMEOUT! Datastore failed to respond after {elapsed:.3f} seconds.")
    except httpx.RequestError as e:
        elapsed = time.time() - start_time
        L.error(f"[TIMING] CONNECTION ERROR after {elapsed:.3f} seconds. Detail: {str(e)}")
    except Exception as e:
        elapsed = time.time() - start_time
        L.error(f"[TIMING] Unexpected fetch failure after {elapsed:.3f} seconds. Detail: {str(e)}")
        
    return []