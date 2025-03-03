from datetime import datetime
from typing import Optional, List
from enum import Enum
from pydantic import BaseModel


class TaskStatus(str, Enum):
    """
    Enumeration of possible task processing states.

    Attributes:
        PENDING: Task is queued but not yet started
        PROCESSING: Task is currently being processed
        COMPLETED: Task has been successfully completed
        FAILED: Task processing has failed
    """

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskResponse(BaseModel):
    """
    Response model for individual task information and results.

    Attributes:
        message_id: Unique identifier for the task
        batch_id: Optional identifier for the batch this task belongs to
        status: Current processing status of the task
        body: Optional response body containing task results
        cached: Whether the result was retrieved from cache
        created_at: Timestamp when the task was created
        started_at: Optional timestamp when processing started
        completed_at: Optional timestamp when processing completed
        duration: Optional processing duration in seconds
        dataset: Optional dataset identifier this task belongs to
        source: Optional metadata about the task source
        completions: Optional model completion data
    """

    message_id: str
    batch_id: Optional[str] = None
    status: TaskStatus
    body: Optional[dict] = None
    cached: bool = False
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration: Optional[int] = None
    dataset: Optional[str] = None
    source: Optional[dict] = None
    completions: Optional[dict] = None

    class Config:
        from_attributes = True


class Batch(BaseModel):
    """
    Model representing a batch of tasks with aggregated statistics.

    Attributes:
        batch_id: Unique identifier for the batch
        batch_status: Current processing status of the batch
        total_tasks: Total number of tasks in the batch
        completed_tasks: Number of successfully completed tasks
        failed_tasks: Number of failed tasks
        pending_tasks: Number of tasks waiting to be processed
        processing_tasks: Number of tasks currently being processed
        cached_tasks: Number of tasks retrieved from cache
        created_at: Timestamp when the batch was created
        started_at: Optional timestamp when batch processing started
        completed_at: Optional timestamp when batch processing completed
        duration: Optional processing duration in seconds
        total_tokens: Total token count (input + output)
        prompt_tokens: Number of input tokens processed
        completion_tokens: Number of output tokens generated
    """

    batch_id: str
    batch_status: TaskStatus
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    pending_tasks: int
    processing_tasks: int
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
    """
    Model representing a paginated list of batches.

    Attributes:
        total: Total number of batches available
        batches: List of batch objects in the current page
    """

    total: int
    batches: List[Batch]

    class Config:
        from_attributes = True


class HealthStatus(str, Enum):
    """
    Enumeration of possible health states for system components.

    Attributes:
        HEALTHY: Component is functioning normally
        UNHEALTHY: Component is experiencing issues
    """

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


class ServiceStatus(BaseModel):
    """
    Model representing the health status of all system components.

    Attributes:
        api: Health status of the API service
        rabbitmq: Health status of the RabbitMQ message broker
        elasticsearch: Health status of the Elasticsearch database
        queue_consumers: Number of active queue consumers
        queue_messages: Number of messages in the queue
    """

    api: HealthStatus = HealthStatus.HEALTHY
    rabbitmq: HealthStatus = HealthStatus.UNHEALTHY
    elasticsearch: HealthStatus = HealthStatus.UNHEALTHY
    task_queue_consumers: int = 0
    task_queue_messages: int = 0
    batch_queue_consumers: int = 0
    batch_queue_messages: int = 0


class HealthResponse(BaseModel):
    """
    Response model for system health check endpoint.

    Attributes:
        status: Overall health status of the system
        services: Detailed status of individual system components
        error: Optional error message if system is unhealthy
    """

    status: HealthStatus
    services: ServiceStatus
    error: Optional[str] = None


class Task(BaseModel):
    """
    Model for individual task submission in bulk operations.

    Attributes:
        custom_id: Client-defined unique identifier for the task
        url: Target URL for the task
        method: HTTP method (POST)
        api_key: Optional API key for authentication
        body: Request body containing task parameters
        dataset: Optional dataset identifier to associate with this task
        source: Optional metadata about the task source
        use_cache: Whether to use cached results if available
        track_progress: Whether to track and report task progress
    """

    custom_id: str
    url: str
    method: str = "POST"
    api_key: Optional[str] = None
    body: dict
    dataset: Optional[str] = None
    source: Optional[dict] = None
    use_cache: bool = True
    track_progress: bool = True


class BulkTaskResponse(BaseModel):
    """
    Response model for bulk task submission.

    Attributes:
        batch_id: Unique identifier for the created batch
        total_tasks: Total number of tasks successfully submitted
    """

    batch_id: str
    total_tasks: int


class TimeSeriesDataPoint(BaseModel):
    """
    Model representing a single data point in a time series of batch metrics.

    Attributes:
        timestamp: The timestamp for this data point
        total_tasks: Total number of tasks in this time bucket
        completed_tasks: Number of successfully completed tasks
        failed_tasks: Number of failed tasks
        cached_tasks: Number of tasks retrieved from cache
        total_tokens: Total token count (input + output)
        prompt_tokens: Number of input tokens processed
        completion_tokens: Number of output tokens generated
        avg_duration_ms: Average task duration in milliseconds
        tokens_per_second: Processing throughput in tokens per second
    """

    timestamp: str
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    cached_tasks: int
    total_tokens: int
    prompt_tokens: int
    completion_tokens: int
    avg_duration_ms: int
    tokens_per_second: float


class StatsSummary(BaseModel):
    """
    Model representing a summary of batch usage statistics.

    Attributes:
        total_tasks: Total number of tasks in the batch
        completed_tasks: Number of successfully completed tasks
        failed_tasks: Number of failed tasks
        cached_tasks: Number of tasks retrieved from cache
        total_tokens: Total token count (input + output)
        completion_tokens: Number of output tokens generated
        average_response_time: Average task response time in milliseconds
        tokens_per_second: Processing throughput in tokens per second
        cache_hit_rate: Ratio of cached tasks to total tasks
    """

    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    cached_tasks: int
    total_tokens: int
    completion_tokens: int
    average_response_time: int
    tokens_per_second: float
    cache_hit_rate: float


class UsageStatsResponse(BaseModel):
    """
    Response model for batch usage statistics.

    Attributes:
        time_range: The requested time range (e.g., "24h", "7d")
        interval: The time bucket interval (e.g., "1h", "1d")
        current_time: Current timestamp when the stats were generated
        time_series: List of time series data points
        summary: Aggregated statistics summary
    """

    time_range: str
    interval: str
    current_time: str
    time_series: List[TimeSeriesDataPoint]
    summary: StatsSummary


class CalendarInterval(str, Enum):
    """
    Valid Elasticsearch calendar intervals as described in the documentation.

    Calendar intervals are time-based intervals that account for irregular time periods like
    months (which can have 28-31 days) or years (which can be leap years).
    """

    MINUTE = "minute"
    MINUTE_SHORT = "1m"
    HOUR = "hour"
    HOUR_SHORT = "1h"
    DAY = "day"
    DAY_SHORT = "1d"
    WEEK = "week"
    WEEK_SHORT = "1w"
    MONTH = "month"
    MONTH_SHORT = "1M"
    QUARTER = "quarter"
    QUARTER_SHORT = "1q"
    YEAR = "year"
    YEAR_SHORT = "1y"


class TaskStatsResponse(BaseModel):
    """
    Response model for task statistics.
    
    Attributes:
        total_tasks: Total number of tasks
        completed_tasks: Number of successfully completed tasks
        failed_tasks: Number of failed tasks
        cached_tasks: Number of tasks retrieved from cache
        processing_tasks: Number of tasks currently being processed
        pending_tasks: Number of tasks waiting to be processed
        total_tokens: Total token count (input + output)
        prompt_tokens: Number of input tokens processed
        completion_tokens: Number of output tokens generated
    """
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    cached_tasks: int
    processing_tasks: int
    pending_tasks: int
    total_tokens: int
    prompt_tokens: int
    completion_tokens: int
