import httpx
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, field_validator
from .models import (
    CalendarInterval,
    TaskResponse,
    Batch,
    BatchList,
    HealthResponse,
    BulkTaskResponse,
    Task,
    TaskStatsResponse,
    TaskStatus,
    UsageStatsResponse,
    HealthStatus,
)
from .exceptions import APIError
import logging
import uuid
from io import BytesIO
import time
import json
import os
import re
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

load_dotenv()
# Configure logging at the top of the file
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Define a consistent color palette
COLORS = {
    "primary": "dodger_blue1",
    "success": "green3",
    "warning": "yellow3",
    "error": "red3",
    "text": "white",
    "muted": "grey70",
    "border": "dodger_blue1",
}


class TimeRange(BaseModel):
    """
    Validator for Elasticsearch date math expressions used in time range queries.
    Supports common formats like:
    - 5m (5 minutes)
    - 2h (2 hours)
    - 1d (1 day)

    Note: The 'now-' prefix is added automatically by the ElasticsearchClient.get_usage_stats method,
    so it should not be included in the time_range parameter.
    """

    time_range: str

    @field_validator("time_range")
    @classmethod
    def validate_time_range(cls, v):
        # Pattern to validate time ranges in the format of Xm, Xh, Xd
        pattern = r"^(\d+)([mhd])$"
        match = re.match(pattern, v)

        if not match:
            raise ValueError(
                "Time range must be in format 'Xm', 'Xh', or 'Xd' "
                "where X is a positive number and m=minutes, h=hours, d=days"
            )

        value, unit = match.groups()
        value = int(value)

        if value <= 0:
            raise ValueError("Time range value must be positive")

        # Validate specific unit limits if needed
        if unit == "m" and value > 1440:  # 24 hours in minutes
            raise ValueError("Minutes should not exceed 1440 (24 hours)")
        elif unit == "h" and value > 720:  # 30 days in hours
            raise ValueError("Hours should not exceed 720 (30 days)")
        elif unit == "d" and value > 365:  # 1 year in days
            raise ValueError("Days should not exceed 365 (1 year)")

        return v


class SynthgenClient:

    def __init__(
        self,
        base_url: str = None,
        api_key: Optional[str] = None,
        timeout: int = 3600,
        config_file: str = None,
    ):
        """Initialize the client with configuration from multiple sources.

        Args:
            base_url: The base URL of the Synthgen API. Defaults to environment variable SYNTHGEN_BASE_URL or "http://localhost:8002".
            api_key: The API key for authentication. Defaults to environment variable SYNTHGEN_API_KEY.
            timeout: Request timeout in seconds. Defaults to 3600.
            config_file: Path to a configuration file. If provided, settings will be loaded from this file.
        """
        # Load from config file if provided
        if config_file:
            self._load_config(config_file)

        # Environment variables take precedence over config file
        self.base_url = base_url or os.environ.get(
            "SYNTHGEN_BASE_URL", "http://localhost:8002"
        )
        self.api_key = api_key or os.environ.get("API_SECRET_KEY")

        logger.debug(f"Initializing SynthgenClient with base_url: {self.base_url}")
        self.timeout = timeout
        self._client = httpx.Client(timeout=timeout, headers=self._get_headers())
        logger.debug("SynthgenClient initialized successfully")

    def _load_config(self, config_file: str) -> None:
        """Load configuration from a JSON file.

        Args:
            config_file: Path to the configuration file

        Raises:
            APIError: If the file cannot be read or contains invalid JSON
        """
        try:
            with open(config_file, "r") as f:
                config = json.load(f)

            # Extract configuration values, setting instance variables
            self.base_url = config.get("base_url")
            self.api_key = config.get("api_key")
            self.timeout = config.get("timeout", 3600)

            # Log successful configuration loading
            logger.debug(f"Loaded configuration from {config_file}")

        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Error loading configuration from {config_file}: {str(e)}")
            raise APIError(f"Failed to load configuration: {str(e)}")

    def _get_headers(self) -> Dict[str, str]:
        """Generate HTTP headers for API requests.

        Returns:
            Dictionary of HTTP headers including authorization if API key is available.
        """
        headers = {
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def close(self):
        """Close the underlying HTTP client to free resources."""
        self._client.close()

    def __enter__(self):
        """Enable context manager support for the client.

        Returns:
            The client instance.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Handle context manager exit by closing the client."""
        self.close()

    def _request(self, method: str, path: str, **kwargs) -> Any:
        """Make an HTTP request to the API with robust retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API endpoint path
            **kwargs: Additional arguments to pass to the request

        Returns:
            Parsed JSON response or None if no content

        Raises:
            APIError: If the request fails after all retry attempts
        """
        url = f"{self.base_url}{path}"
        max_retries = 10
        base_delay = 5  # base delay in seconds

        for attempt in range(max_retries):
            try:
                response = self._client.request(
                    method, url, timeout=self.timeout, **kwargs
                )
                response.raise_for_status()
                return response.json() if response.content else None
            except httpx.HTTPStatusError as e:
                # Don't retry on 401 Unauthorized errors
                if e.response.status_code == 401:
                    logger.error(f"Authentication error (401 Unauthorized): {str(e)}")
                    raise APIError(
                        "Authentication failed. Please check your API key.",
                        status_code=e.response.status_code,
                        response=e.response,
                    )

                if attempt < max_retries - 1:
                    logger.warning(
                        f"HTTPStatusError {e.response.status_code} encountered on attempt {attempt + 1} of {max_retries}. "
                        f"Retrying in {base_delay} seconds..."
                    )
                    time.sleep(base_delay)
                    continue
                raise APIError(
                    str(e), status_code=e.response.status_code, response=e.response
                )
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Exception '{str(e)}' encountered on attempt {attempt + 1} of {max_retries}. "
                        f"Retrying in {base_delay} seconds..."
                    )
                    time.sleep(base_delay)
                    continue
                raise APIError(str(e))

    def get_task(self, message_id: str) -> TaskResponse:
        """Get task status and result.

        Args:
            message_id: The unique identifier of the task

        Returns:
            TaskResponse object containing task details and results
        """
        response = self._request("GET", f"/api/v1/tasks/{message_id}")

        return TaskResponse.model_validate(response)

    def delete_task(self, message_id: str) -> None:
        """Delete a task from the system.

        Args:
            message_id: The unique identifier of the task to delete
        """
        self._request("DELETE", f"/api/v1/tasks/{message_id}")

    def get_batch(self, batch_id: str) -> Batch:
        """Get batch status and metadata.

        Args:
            batch_id: The unique identifier of the batch

        Returns:
            Batch object containing batch details
        """
        response = self._request("GET", f"/api/v1/batches/{batch_id}")
        return Batch.model_validate(response)

    def get_batches(self) -> BatchList:
        """List all batches in the system.

        Returns:
            BatchList object containing all batches
        """
        response = self._request("GET", "/api/v1/batches")
        return BatchList.model_validate(response)

    def delete_batch(self, batch_id: str) -> None:
        """Delete a batch from the system.

        Args:
            batch_id: The unique identifier of the batch to delete
        """
        self._request("DELETE", f"/api/v1/batches/{batch_id}")

    def check_health(self) -> HealthResponse:
        """Check system health status of all services.

        Returns:
            HealthResponse object containing status of all system components
        """
        response = self._request("GET", "/health")
        return HealthResponse.model_validate(response)

    def create_batch(
        self, tasks: List[Task], chunk_size: int = 1000
    ) -> BulkTaskResponse:
        """Create a new batch from a list of tasks with chunking logic.

        Args:
            tasks: List of Task objects containing task details
            chunk_size: Number of tasks to process in each chunk (default: 1000)

        Returns:
            BulkTaskResponse containing the batch_id and number of rows processed

        Raises:
            APIError: If an error occurs during conversion or API request
        """
        try:
            logger.debug(f"Starting create_batch with {len(tasks)} tasks")
            logger.debug(f"Chunk size: {chunk_size}")

            from rich.progress import (
                Progress,
                SpinnerColumn,
                BarColumn,
                TextColumn,
                TaskProgressColumn,
            )
            from rich.console import Console
            from rich.panel import Panel
            from rich.live import Live

            console = Console()
            batch_id = str(uuid.uuid4())
            logger.debug(f"Generated batch_id: {batch_id}")

            total_chunks = (len(tasks) + chunk_size - 1) // chunk_size
            logger.debug(f"Will process {total_chunks} chunks")

            total_processed = 0

            # Create progress display
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
            )
            upload_task = progress.add_task(
                "[cyan]Uploading chunks...", total=total_chunks
            )

            # Use Live display with Panel
            with Live(
                Panel(
                    progress,
                    title=f"[bold {COLORS['primary']}]Batch Upload Progress[/]",
                    border_style=COLORS["border"],
                    padding=(1, 2),
                ),
                console=console,
                refresh_per_second=4,
            ):
                # Process tasks in chunks
                for chunk_index, i in enumerate(range(0, len(tasks), chunk_size), 1):
                    progress.update(
                        upload_task,
                        description=f"[cyan]Uploading chunk {chunk_index}/{total_chunks}",
                        advance=1,
                    )

                    chunk_start = i
                    chunk_end = i + chunk_size
                    logger.debug(
                        f"Processing chunk {chunk_index}/{total_chunks} (indexes {chunk_start}:{chunk_end})"
                    )

                    # Create chunk
                    chunk = tasks[chunk_start:chunk_end]
                    logger.debug(f"Created chunk with {len(chunk)} tasks")

                    # Convert to JSONL
                    jsonl_content = []
                    for task in chunk:
                        jsonl_content.append(task.model_dump_json())
                    jsonl_data = "\n".join(jsonl_content)
                    logger.debug(f"JSONL data created, size: {len(jsonl_data)} bytes")

                    # Prepare request
                    params = {"batch_id": batch_id}
                    file_obj = BytesIO(jsonl_data.encode("utf-8"))
                    files = {
                        "file": ("batch.jsonl", file_obj, "application/x-jsonlines")
                    }

                    # Send request
                    logger.debug(f"Sending chunk {chunk_index} to API")
                    try:
                        response = self._request(
                            "POST", "/api/v1/batches", files=files, params=params
                        )
                        logger.debug(f"API response received for chunk {chunk_index}")

                        chunk_response = BulkTaskResponse.model_validate(response)
                        total_processed += chunk_response.total_tasks
                        logger.debug(
                            f"Chunk {chunk_index} processed successfully. Total processed: {total_processed}"
                        )

                    except Exception as chunk_error:
                        logger.error(
                            f"Error processing chunk {chunk_index}: {str(chunk_error)}",
                            exc_info=True,
                        )
                        raise

            logger.debug(f"All chunks completed. Total processed: {total_processed}")
            return BulkTaskResponse(batch_id=batch_id, total_tasks=total_processed)

        except Exception as e:
            logger.error(f"Error in create_batch: {str(e)}", exc_info=True)
            raise APIError(f"Error creating batch: {e}")

    def monitor_batch(
        self,
        tasks: Optional[List[Task]] = None,
        batch_id: Optional[str] = None,
        cost_by_1m_input_token: float = 0.0,
        cost_by_1m_output_token: float = 0.0,
    ) -> List[TaskResponse]:
        """Monitor batch task processing with real-time progress display.

        Provides a rich UI showing batch progress, token usage, and cost estimates.
        Either submits a new batch using the provided tasks or monitors an existing batch.

        Args:
            tasks: List of Task objects to submit as a new batch (optional if batch_id is provided)
            batch_id: ID of an existing batch to monitor (optional if tasks is provided)
            cost_by_1m_input_token: Cost per million input tokens for cost calculation
            cost_by_1m_output_token: Cost per million output tokens for cost calculation

        Returns:
            List of TaskResponse objects containing completed task results

        Raises:
            ValueError: If neither tasks nor batch_id is provided
        """
        from rich.console import Group
        from rich.live import Live
        from rich.progress import (
            Progress,
            SpinnerColumn,
            BarColumn,
            TextColumn,
            TaskProgressColumn,
        )
        import time

        console = Console()

        # 1. Health Check Display with harmonized styling
        health = self.check_health()

        # Create a single comprehensive health table
        health_table = Table(
            show_header=True, header_style=f"bold {COLORS['primary']}", box=box.ROUNDED
        )
        health_table.add_column("Component", justify="left", style=COLORS["text"])
        health_table.add_column("Status", style=COLORS["text"])

        def get_status_style(status: bool) -> str:
            return COLORS["success"] if status else COLORS["error"]

        def status_indicator(service_status):
            is_healthy = service_status == HealthStatus.HEALTHY
            color = get_status_style(is_healthy)
            status_text = "Online" if is_healthy else "Offline"
            return f"[{color}]●[/] {status_text}"

        # API status
        health_table.add_row("[bold]API[/]", status_indicator(health.services.api))

        # Elasticsearch status
        health_table.add_row(
            "[bold]Elasticsearch[/]", status_indicator(health.services.elasticsearch)
        )

        # RabbitMQ status with queue information
        rabbitmq_status = status_indicator(health.services.rabbitmq)
        task_queue_info = f"Task Queue: {health.services.task_queue_consumers} consumers, {health.services.task_queue_messages} msgs"
        batch_queue_info = f"Batch Queue: {health.services.batch_queue_consumers} consumers, {health.services.batch_queue_messages} msgs"

        health_table.add_row(
            "[bold]RabbitMQ[/]",
            f"{rabbitmq_status}\n[{COLORS['muted']}]{task_queue_info}\n{batch_queue_info}[/]",
        )

        # Overall system status
        system_status = (
            "Healthy" if health.status == HealthStatus.HEALTHY else "Unhealthy"
        )
        system_status_color = get_status_style(health.status == HealthStatus.HEALTHY)
        health_table.add_row(
            "[bold]System Status[/]",
            f"[{system_status_color}]{system_status}[/]"
            + (f"\n[{COLORS['error']}]{health.error}[/]" if health.error else ""),
        )

        console.print(
            Panel(
                health_table,
                title=f"[bold {COLORS['primary']}]System Health[/]",
                border_style=COLORS["border"],
                padding=(1, 2),
            )
        )

        # 2. Submit batch or use existing batch_id
        if batch_id is None:
            if tasks is None:
                raise ValueError("Either tasks or batch_id must be provided")

            bulk_response = self.create_batch(tasks)
            batch_id = bulk_response.batch_id
            total_tasks = bulk_response.total_tasks
        else:
            console.print(
                f"\n[bold yellow]Monitoring existing batch: {batch_id}[/bold yellow]"
            )
            batch = self.get_batch(batch_id)
            total_tasks = batch.total_tasks

        # 3. Setup progress monitoring with harmonized styling
        progress = Progress(
            SpinnerColumn(style=COLORS["primary"]),
            TextColumn(f"[{COLORS['text']}]{{task.description}}[/]"),
            BarColumn(
                complete_style=COLORS["success"], finished_style=COLORS["success"]
            ),
            TaskProgressColumn(style=COLORS["text"]),
        )
        progress_task = progress.add_task("Processing batch...", total=total_tasks)

        # Track last update time to prevent too frequent API calls
        last_update = 0
        cached_batch = None
        UPDATE_INTERVAL = 2  # seconds

        def render_dashboard() -> Panel:
            nonlocal last_update, cached_batch
            current_time = time.time()
            if current_time - last_update >= UPDATE_INTERVAL:
                cached_batch = self.get_batch(batch_id)
                last_update = current_time
            batch = cached_batch

            # Create status table with consistent styling
            status_table = Table(show_header=False, box=None, padding=(0, 2))
            status_table.add_column("Metric", justify="right", style=COLORS["muted"])
            status_table.add_column("Value", justify="left", style=COLORS["text"])

            # Calculate metrics
            duration = getattr(batch, "duration", 0) or 0
            hours, remainder = divmod(duration, 3600)
            minutes, seconds = divmod(remainder, 60)
            duration_str = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

            total_tokens = getattr(batch, "total_tokens", 0) or 0
            prompt_tokens = getattr(batch, "prompt_tokens", 0) or 0
            completion_tokens = getattr(batch, "completion_tokens", 0) or 0

            # Ensure we're using float values for cost calculations
            input_cost = (float(prompt_tokens) / 1_000_000) * float(
                cost_by_1m_input_token
            )
            output_cost = (float(completion_tokens) / 1_000_000) * float(
                cost_by_1m_output_token
            )
            total_cost = input_cost + output_cost

            # Add rows with consistent formatting
            metrics = [
                ("Completed", f"[{COLORS['success']}]{batch.completed_tasks:,}[/]"),
                ("Pending", f"[{COLORS['warning']}]{batch.pending_tasks:,}[/]"),
                ("Processing", f"[{COLORS['primary']}]{batch.processing_tasks:,}[/]"),
                ("Failed", f"[{COLORS['error']}]{batch.failed_tasks:,}[/]"),
                ("Cached", f"[{COLORS['muted']}]{batch.cached_tasks:,}[/]"),
                ("Total", f"[bold {COLORS['text']}]{batch.total_tasks:,}[/]"),
                ("Duration", duration_str),
                ("Prompt Tokens", f"{prompt_tokens:,}"),
                ("Completion Tokens", f"{completion_tokens:,}"),
                ("Total Tokens", f"[bold]{total_tokens:,}[/]"),
                ("Input Cost", f"${input_cost:.4f}"),
                ("Output Cost", f"${output_cost:.4f}"),
                ("Total Cost", f"[bold]${total_cost:.4f}[/]"),
            ]

            for metric, value in metrics:
                status_table.add_row(f"{metric}:", value)

            return Panel(
                Group(progress, status_table),
                title=f"[bold {COLORS['primary']}]Batch ID: {batch_id}[/]",
                border_style=COLORS["border"],
                padding=(1, 2),
            )

        # Using transient=False so that the final dashboard remains visible
        with Live(
            render_dashboard(),
            refresh_per_second=2,
            console=console,
            transient=False,  # Changed from True to False
        ) as live:
            while True:
                batch = self.get_batch(batch_id)
                progress.update(progress_task, completed=batch.completed_tasks)
                live.update(render_dashboard())
                tasks_done = batch.completed_tasks + batch.failed_tasks
                if tasks_done >= total_tasks or batch.batch_status in [
                    "COMPLETED",
                    "FAILED",
                ]:
                    break
                time.sleep(2)

        console.print("\n[bold green]Batch processing completed![/bold green]")

        # 4. Return results
        tasks_data = self.get_batch_tasks(batch_id)
        return tasks_data

    def get_batch_tasks(
        self, batch_id: str, task_status: TaskStatus = TaskStatus.COMPLETED
    ) -> List[TaskResponse]:
        """Stream tasks for a given batch while displaying download progress.

        Args:
            batch_id: The unique identifier of the batch
            task_status: Filter tasks by status (default: COMPLETED)

        Returns:
            List of TaskResponse objects matching the specified criteria
        """
        from rich.progress import (
            Progress,
            SpinnerColumn,
            TextColumn,
            BarColumn,
            TaskProgressColumn,
        )

        # Fetch metadata to know the total number of tasks
        batch = self.get_batch(batch_id)
        total_tasks = getattr(batch, "total_tasks", None)

        params = {"task_status": task_status.value}
        url = f"{self.base_url}/api/v1/batches/{batch_id}/tasks"

        tasks_accumulated: List[TaskResponse] = []

        with self._client.stream(
            "GET", url, params=params, headers=self._get_headers()
        ) as response:
            response.raise_for_status()
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
            ) as progress:
                progress_task = progress.add_task(
                    "Downloading tasks...", total=total_tasks
                )

                for line in response.iter_lines():
                    if line:
                        try:
                            chunk = json.loads(line.decode("utf-8"))
                        except AttributeError:
                            chunk = json.loads(line)
                        tasks = chunk.get("tasks", [])
                        # Convert each task to TaskResponse
                        for task in tasks:
                            tasks_accumulated.append(TaskResponse.model_validate(task))
                        progress.advance(progress_task, len(tasks))

        return tasks_accumulated

    def get_batch_stats(
        self,
        batch_id: str,
        time_range: str = "24h",
        interval: CalendarInterval = CalendarInterval.HOUR_SHORT,
    ) -> UsageStatsResponse:
        """Get time-bucketed usage statistics for a batch.

        Args:
            batch_id: The unique identifier of the batch
            time_range: Time range for statistics without 'now-' prefix (e.g., "5m", "2h", "7d")
            interval: Time bucket size using Elasticsearch calendar intervals

        The interval parameter accepts the following Elasticsearch calendar intervals:
        - minute, 1m: One minute interval
        - hour, 1h: One hour interval
        - day, 1d: One day interval
        - week, 1w: One week interval
        - month, 1M: One month interval
        - quarter, 1q: One quarter interval
        - year, 1y: One year interval

        The time_range parameter format (without 'now-' prefix, which is added automatically):
        - Xm: X minutes (e.g., 30m for the last 30 minutes)
        - Xh: X hours (e.g., 6h for the last 6 hours)
        - Xd: X days (e.g., 7d for the last 7 days)

        Returns:
            UsageStatsResponse object containing batch statistics and time series data

        Raises:
            APIError: If the statistics cannot be retrieved or parameters are invalid
        """
        logger.debug(
            f"Fetching stats for batch {batch_id} with time_range={time_range}, interval={interval}"
        )

        try:
            # Validate the time_range parameter
            TimeRange(time_range=time_range)

            params = {"time_range": time_range, "interval": interval.value}

            response = self._request(
                "GET", f"/api/v1/batches/{batch_id}/stats", params=params
            )
            logger.debug(f"Successfully retrieved stats for batch {batch_id}")
            return UsageStatsResponse.model_validate(response)

        except ValueError as ve:
            # Handle validation errors from the TimeRange validator
            logger.error(f"Invalid time_range parameter: {str(ve)}")
            raise APIError(f"Invalid time_range parameter: {str(ve)}")
        except Exception as e:
            logger.error(f"Failed to fetch stats for batch {batch_id}: {str(e)}")
            raise APIError(f"Failed to fetch batch statistics: {str(e)}")

    def get_task_stats(self) -> TaskStatsResponse:
        """Get task statistics for the current batch.

        Returns:
            TaskStatsResponse object containing task statistics
        """
        url = "/api/v1/tasks/stats"
        response = self._request("GET", url, headers=self._get_headers())
        return TaskStatsResponse.model_validate(response)
