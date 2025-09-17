from enum import Enum
from typing import List, Optional, Dict
from pydantic import BaseModel, Field
from utils import FilterType


class CrawlRequest(BaseModel):
    urls: List[str] = Field(min_length=1, max_length=100)
    temp_task_id: str
    operation_data: Optional[Dict] = Field(default_factory=dict)
    browser_config: Optional[Dict] = Field(default_factory=dict)
    crawler_config: Optional[Dict] = Field(default_factory=dict)

class MarkdownRequest(BaseModel):
    """Request body for the /md endpoint."""
    urls: List[str]    = Field(...,  min_length=1, max_length=100, description="Absolute http/https URLs to fetch")
    f:   FilterType    = Field(FilterType.FIT,
                                        description="Content‑filter strategy: FIT, RAW, BM25, or LLM")
    q:   Optional[str] = Field(None,  description="Query string used by BM25/LLM filters")
    c:   Optional[str] = Field("0",   description="Cache‑bust / revision counter")
    browser_config: Optional[Dict] = Field(default_factory=dict, description="Browser configuration for the crawler")


class RawCode(BaseModel):
    code: str

class HTMLRequest(BaseModel):
    url: str
    
class ScreenshotRequest(BaseModel):
    url: str
    screenshot_wait_for: Optional[float] = 2
    output_path: Optional[str] = None

class PDFRequest(BaseModel):
    url: str
    output_path: Optional[str] = None


class JSEndpointRequest(BaseModel):
    url: str
    scripts: List[str] = Field(
        ...,
        description="List of separated JavaScript snippets to execute"
    )


class OpenAIModelFee(BaseModel):
    model_name: str = Field(..., description="Name of the OpenAI model.")
    input_fee: str = Field(..., description="Fee for input token for the OpenAI model.")
    output_fee: str = Field(
        ..., description="Fee for output token for the OpenAI model."
    )


class CrawlStorageMetadata(BaseModel):
    created_At: int
    updated_At: Optional[int] = None
    file_compressed_size: int
    file_size: int
    file_name: str
    key_name: str

class CrawlStorage(BaseModel):
    error: Optional[str] = None
    metadata: CrawlStorageMetadata
    url: str
class AIModel(BaseModel):
    name: str
    code: str

class Author(BaseModel):
    uid: str
    displayName: str

class CrawlJobType(str, Enum):
    PLAYGROUND = "playground"
    OPERATION = "operation"

class CrawlOperation(BaseModel):
    id: Optional[str] = None
    urls: List[str]
    author: Author
    name: Optional[str] = None
    color: str
    urlPath: Optional[str] = None
    type: str # Replace with Enum if you have a taskType enum CrawlJobType
    modelAI: Optional[AIModel] = None
    created_At: int
    updated_At: Optional[int] = None
    scheduled_At: Optional[int] = None
    prompt: Optional[str] = None
    status: str  # Replace with Enum if you have a taskStatus enum
    metadataId: Optional[str] = None  # CrawlPack
    error: Optional[str] = None
    storage: Optional[List[CrawlStorage]] = None

# ONE OPERATION DEFINED AS MULTIPLE TASKS
class OperationResult(BaseModel):
    operation_id: str
    machine_id: str
    duration: float
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    peak_memory: Optional[float] = None
    memory_used: Optional[float] = None
    status: str
    urls_processed: Optional[int] = None
    error: Optional[str] = None

class SystemStats(BaseModel):
    cpu_usage: float
    memory_usage: float
    queue_length: int
    error_rate: float

class ResourceStatus(BaseModel):
    memory_ok: bool
    cpu_ok: bool
    memory_usage: float
    cpu_usage: float
    per_core_usage: list[float]

class CoreMetrics(BaseModel):
    core_id: int
    cpu_usage: float
    memory_usage: float

# FIXME: This class likely represents a product entity and inherits from a base model class.
class Product(BaseModel):
    name: str
    price: str

class UserAbortException(Exception):
    """Raised when a user aborts the operation."""
    pass