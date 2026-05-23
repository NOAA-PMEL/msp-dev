import httpx
import logging
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
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

# Shared memory cache: up to 128 unique endpoints, stored for 5 minutes (300s)
registry_cache = TTLCache(maxsize=128, ttl=300)

@cached(cache=registry_cache)
def get_registry_data(endpoint: str):
    """Safely fetches data using httpx, heavily cached to protect the datastore."""
    url = f"http://{datastore_url}/{endpoint}"
    
    L.debug("Cache miss! Re-fetching registry data from datastore", extra={"fetch_url": url})
    
    try:
        with httpx.Client() as client:
            response = client.get(url, timeout=5.0)
            
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            L.debug("Successfully parsed registry items", extra={"query_endpoint": endpoint, "item_count": len(results)})
            return results
        else:
            L.error("API returned non-200 code", extra={"http_status": response.status_code, "err_body": response.text})
            
    except httpx.RequestError as e:
        L.error("CONNECTION ERROR during fetch", extra={"fetch_url": url, "failure_detail": str(e)})
    except Exception as e:
        L.error("Unexpected fetch failure", extra={"query_endpoint": endpoint, "failure_detail": str(e)})
        
    return []