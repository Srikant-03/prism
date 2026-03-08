from typing import Any
from pydantic import BaseModel
from profiling.profiling_models import ProfilingResult

class SchemaOverrideRequest(BaseModel):
    column: str
    new_type: str

class CellRepairRequest(BaseModel):
    repairs: list[dict[str, Any]]
