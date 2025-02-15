import httpx
import json
from typing import Optional, Dict, Any, List
from .models import (
    TaskResponse,
    Batch,
    BatchList,
    TaskList,
    TaskRequest,
    HealthResponse,
    BulkTaskResponse,
    TaskListSubmission,
)
from .exceptions import APIError
import logging
import uuid
from io import BytesIO
import time

# Configure logging at the top of the file
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SynthgenClient:

    def __init__(
        self, base_url: str, api_key: Optional[str] = None, timeout: int = 300
    ):
        logger.debug(f"Initializing SynthgenClient with base_url: {base_url}")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._client = httpx.Client(timeout=timeout, headers=self._get_headers())
        logger.debug("SynthgenClient initialized successfully")

    def _get_headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def close(self):
        """Close the underlying HTTP client"""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _request(self, method: str, path: str, **kwargs) -> Any:
        """Make an HTTP request to the API with robust retry logic that adds a delay upon each exception."""
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

    def create_task(self, task: TaskRequest) -> str:
        """Create a new task"""
        response = self._request("POST", "/api/v1/tasks", json=task.model_dump())
        return response["message_id"]

    def get_task(self, message_id: str) -> TaskResponse:
        """Get task status and result"""
        response = self._request("GET", f"/api/v1/tasks/{message_id}")

        return TaskResponse.model_validate(response)

    def delete_task(self, message_id: str) -> None:
        """Delete a task"""
        self._request("DELETE", f"/api/v1/tasks/{message_id}")

    def get_batch(self, batch_id: str) -> Batch:
        """Get batch status"""
        response = self._request("GET", f"/api/v1/batches/{batch_id}")
        return Batch.model_validate(response)

    def get_batches(self) -> BatchList:
        """List all batches"""
        response = self._request("GET", "/api/v1/batches")
        return BatchList.model_validate(response)

    def get_batch_tasks(self, batch_id: str) -> TaskList:
        """Get tasks in a batch"""
        response = self._request(
            "GET",
            f"/api/v1/batches/{batch_id}/tasks",
        )
        return TaskList.model_validate(response)

    def delete_batch(self, batch_id: str) -> None:
        """Delete a batch"""
        self._request("DELETE", f"/batches/{batch_id}")

    def check_health(self) -> HealthResponse:
        """Check system health status"""
        response = self._request("GET", "/health")
        return HealthResponse.model_validate(response)

    def create_batch(
        self, tasks: TaskListSubmission, chunk_size: int = 1000
    ) -> BulkTaskResponse:
        """Create a new batch from TaskListSubmission with chunking logic

        Args:
            tasks: TaskListSubmission object containing a list of TaskSubmission objects
                  Each TaskSubmission contains task details like custom_id, method, url, etc.
            chunk_size: Number of tasks to process in each chunk (default: 1000)

        Returns:
            BulkTaskResponse containing the batch_id and number of rows processed

        Raises:
            APIError: If an error occurs during conversion or API request
        """
        try:
            logger.debug(f"Starting create_batch with {len(tasks.tasks)} tasks")
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

            total_chunks = (len(tasks.tasks) + chunk_size - 1) // chunk_size
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
                Panel(progress, title="Batch Upload Progress", border_style="blue"),
                console=console,
                refresh_per_second=4,
            ):
                # Process tasks in chunks
                for chunk_index, i in enumerate(
                    range(0, len(tasks.tasks), chunk_size), 1
                ):
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
                    chunk = TaskListSubmission(tasks=tasks.tasks[chunk_start:chunk_end])
                    logger.debug(f"Created chunk with {len(chunk.tasks)} tasks")

                    # Convert to JSONL
                    jsonl_content = []
                    for task in chunk.tasks:
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
        tasks: Optional[TaskListSubmission] = None,
        batch_id: Optional[str] = None,
        cost_by_1m_input_token: float = 0.0,  # Cost per 1M input tokens
        cost_by_1m_output_token: float = 0.0,  # Cost per 1M output tokens
    ) -> TaskList:
        """
        Run an interactive dashboard that monitors batch task processing.

        Args:
            tasks: Optional TaskListSubmission object containing the tasks to process.
                  Required if batch_id is not provided.
            batch_id: Optional existing batch ID to monitor.
                     If provided, tasks parameter will be ignored.
            cost_by_1m_input_token: Cost per 1M input tokens (default: 0.0)
            cost_by_1m_output_token: Cost per 1M output tokens (default: 0.0)

        Returns:
            List[TaskResponse]: List of completed tasks with their results

        Raises:
            ValueError: If neither tasks nor batch_id is provided
        """
        from rich.console import Console, Group
        from rich.live import Live
        from rich.table import Table
        from rich.panel import Panel
        from rich.progress import (
            Progress,
            SpinnerColumn,
            BarColumn,
            TextColumn,
            TaskProgressColumn,
        )
        import time

        console = Console()

        # 1. Health Check Display
        health = self.check_health()
        health_table = Table(
            title="Health Check", show_header=True, header_style="bold cyan"
        )
        health_table.add_column("Service", justify="center")
        health_table.add_column("Status", justify="center", style="green")
        health_table.add_column("Details", justify="center")
        health_table.add_row("API", str(health.services.api.value), "")
        health_table.add_row(
            "RabbitMQ",
            str(health.services.rabbitmq.value),
            f"Messages: {health.services.queue_messages}",
        )
        health_table.add_row("Postgres", str(health.services.postgres.value), "")
        health_table.add_row(
            "Queue Consumers", "", str(health.services.queue_consumers)
        )
        console.print(
            Panel(health_table, title="[bold blue]System Health", border_style="blue")
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

        # 3. Setup progress monitoring
        progress = Progress(
            SpinnerColumn(style="green"),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=None, style="green"),
            TaskProgressColumn(),
        )
        progress_task = progress.add_task("Monitoring Batch...", total=total_tasks)

        # Track last update time to prevent too frequent API calls
        last_update = 0
        cached_batch = None
        UPDATE_INTERVAL = 2  # seconds

        # Live dashboard render (make persistent by setting transient=False)
        def render_dashboard() -> Panel:
            nonlocal last_update, cached_batch
            current_time = time.time()
            if current_time - last_update >= UPDATE_INTERVAL:
                cached_batch = self.get_batch(batch_id)
                last_update = current_time
            batch = cached_batch
            status_table = Table(show_header=False, box=None, padding=(0, 4))
            status_table.add_column("Metric", justify="right", style="bold")
            status_table.add_column("Value", justify="left", style="white")
            duration = getattr(batch, "duration", 0) or 0
            hours = int(duration // 3600)
            minutes = int((duration % 3600) // 60)
            seconds = int(duration % 60)
            duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            total_tokens = getattr(batch, "total_tokens", 0) or 0
            prompt_tokens = getattr(batch, "prompt_tokens", 0) or 0
            completion_tokens = getattr(batch, "completion_tokens", 0) or 0
            input_cost = (prompt_tokens / 1_000_000) * cost_by_1m_input_token
            output_cost = (completion_tokens / 1_000_000) * cost_by_1m_output_token
            total_cost = input_cost + output_cost
            metrics = [
                ("Completed", str(batch.completed_tasks)),
                ("Pending", str(batch.pending_tasks)),
                ("Processing", str(batch.processing_tasks)),
                ("Failed", str(batch.failed_tasks)),
                ("Cached", str(batch.cached_tasks)),
                ("Total", str(batch.total_tasks)),
                ("Duration", duration_str),
                ("Prompt Tokens", f"{prompt_tokens:,}"),
                ("Completion Tokens", f"{completion_tokens:,}"),
                ("Total Tokens", f"{total_tokens:,}"),
                ("Input Cost", f"$ {input_cost:.4f}"),
                ("Output Cost", f"$ {output_cost:.4f}"),
                ("Total Cost", f"$ {total_cost:.4f}"),
            ]
            for metric, value in metrics:
                status_table.add_row(f"{metric}:", value)
            dashboard_group = Group(progress, status_table)
            return Panel(
                dashboard_group,
                title=f"Batch ID: {batch_id}",
                border_style="bright_blue",
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
