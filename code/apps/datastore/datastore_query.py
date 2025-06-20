from typing import List
from pydantic import BaseModel

class DataStoreQuery(BaseModel):
    sensor_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    last_n_seconds: int | None
    variable: List[str] | None = None
