from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel

class TaskStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class Message(BaseModel):
    role: str
    content: str

class TaskRequest(BaseModel):
    model: str
    messages: List[Message]

class Task(BaseModel):
    message_id: str
    batch_id: Optional[str] = None
    status: TaskStatus
    payload: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    cached: bool = False
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration: Optional[int] = None
    queue_position: Optional[int] = None

    class Config:
        from_attributes = True

class Batch(BaseModel):
    batch_id: str
    batch_status: TaskStatus
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    pending_tasks: int
    cached_tasks: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration: Optional[int] = None
    total_tokens: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0

    class Config:
        from_attributes = True

class BatchList(BaseModel):
    total: int
    page: int
    page_size: int
    batches: List[Batch]

    class Config:
        from_attributes = True

class TaskList(BaseModel):
    total: int
    page: int
    page_size: int
    tasks: List[Task]


    class Config:
        from_attributes = True

class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"

class ServiceStatus(BaseModel):
    api: HealthStatus = HealthStatus.HEALTHY
    rabbitmq: HealthStatus = HealthStatus.UNHEALTHY
    postgres: HealthStatus = HealthStatus.UNHEALTHY
    queue_consumers: int = 0
    queue_messages: int = 0

class HealthResponse(BaseModel):
    status: HealthStatus
    services: ServiceStatus
    error: Optional[str] = None 