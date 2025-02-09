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

logger = logging.getLogger(__name__)


class SynthgenClient:

    def __init__(
        self, base_url: str, api_key: Optional[str] = None, timeout: int = 300
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._client = httpx.Client(timeout=timeout, headers=self._get_headers())

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
        """Make an HTTP request to the API with retry logic for 404 errors"""
        url = f"{self.base_url}{path}"
        max_retries = 10
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                response = self._client.request(
                    method, url, timeout=self.timeout, **kwargs
                )
                response.raise_for_status()
                return response.json() if response.content else None
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404 and attempt < max_retries - 1:
                    logger.warning(
                        f"404 error,attempt {attempt+1} of {max_retries}, retrying in {retry_delay} seconds"
                    )
                    import time

                    time.sleep(retry_delay)
                    continue
                raise APIError(
                    str(e), status_code=e.response.status_code, response=e.response
                )
            except Exception as e:
                raise APIError(str(e))

    def create_task(self, task: TaskRequest) -> str:
        """Create a new task"""
        response = self._request("POST", "/api/v1/tasks", json=task.model_dump())
        return response["message_id"]

    def get_task(self, message_id: str) -> TaskResponse:
        """Get task status and result"""
        response = self._request("GET", f"/api/v1/tasks/{message_id}")

        # Parse the payload and result strings into dictionaries
        if isinstance(response["payload"], str):
            response["payload"] = json.loads(response["payload"].replace("'", '"'))
        if isinstance(response["result"], str):
            response["result"] = json.loads(response["result"].replace("'", '"'))

        return TaskResponse.model_validate(response)

    def delete_task(self, message_id: str) -> None:
        """Delete a task"""
        self._request("DELETE", f"/api/v1/tasks/{message_id}")

    def get_batch(self, batch_id: str) -> Batch:
        """Get batch status"""
        response = self._request("GET", f"/api/v1/batches/{batch_id}")
        return Batch.model_validate(response)

    def get_batches(self, page: int = 1, page_size: int = 50) -> BatchList:
        """List all batches"""
        response = self._request(
            "GET", "/api/v1/batches", params={"page": page, "page_size": page_size}
        )
        return BatchList.model_validate(response)

    def get_batches_all(self, page_size: int = 50) -> List[Batch]:
        """List all batches with automatic pagination

        Args:
            page_size: Number of items per page (default: 50)

        Returns:
            BatchList containing all batches across all pages
        """
        all_batches = []
        page = 1

        while True:
            response = self._request(
                "GET", "/api/v1/batches", params={"page": page, "page_size": page_size}
            )
            batch_list = BatchList.model_validate(response)

            if not batch_list.batches:
                break

            all_batches.extend(batch_list.batches)
            page += 1

        return all_batches

    def get_batch_tasks(
        self, batch_id: str, page: int = 1, page_size: int = 50
    ) -> TaskList:
        """Get tasks in a batch"""
        response = self._request(
            "GET",
            f"/api/v1/batches/{batch_id}/tasks",
            params={"page": page, "page_size": page_size},
        )
        return TaskList.model_validate(response)

    def get_batch_tasks_all(
        self, batch_id: str, page_size: int = 50
    ) -> List[TaskResponse]:
        """Get all tasks in a batch with automatic pagination"""
        all_tasks = []
        page = 1
        while True:
            response = self._request(
                "GET",
                f"/api/v1/batches/{batch_id}/tasks",
                params={"page": page, "page_size": page_size},
            )
            task_list = TaskList.model_validate(response)
            if not task_list.tasks:
                break
            all_tasks.extend(task_list.tasks)
            page += 1
        return all_tasks

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

        Returns:
            BulkTaskResponse containing the batch_id and number of rows processed

        Raises:
            APIError: If an error occurs during conversion or API request
        """
        try:
            batch_id = str(uuid.uuid4())
            total_processed = 0

            # Process tasks in chunks
            for i in range(0, len(tasks.tasks), chunk_size):
                chunk = tasks.tasks[i : i + chunk_size]

                # Convert chunk to JSONL format
                jsonl_content = []
                for task in chunk:
                    jsonl_content.append(task.model_dump_json())
                jsonl_data = "\n".join(jsonl_content)

                # Add batch_id
                params = {"batch_id": batch_id}

                # Create in-memory file-like object
                from io import BytesIO

                file_obj = BytesIO(jsonl_data.encode("utf-8"))
                files = {"file": ("batch.jsonl", file_obj, "application/x-jsonlines")}

                response = self._request(
                    "POST", "/api/v1/batches", files=files, params=params
                )
                chunk_response = BulkTaskResponse.model_validate(response)
                total_processed += chunk_response.total_tasks

                logger.info(
                    f"Processed chunk of {len(chunk)} tasks (total: {total_processed})"
                )

            # Return the final response with total processed tasks
            return BulkTaskResponse(batch_id=batch_id, total_tasks=total_processed)

        except Exception as e:
            raise APIError(f"Error creating batch: {e}")

    def monitor_batch(
        self,
        tasks: Optional[TaskListSubmission] = None,
        batch_id: Optional[str] = None,
        cost_by_1m_input_token: float = 0.0,  # Cost per 1M input tokens
        cost_by_1m_output_token: float = 0.0,  # Cost per 1M output tokens
    ) -> List[TaskResponse]:
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

            console.print("\n[bold yellow]Submitting batch tasks...[/bold yellow]")
            bulk_response = self.create_batch(tasks)
            batch_id = bulk_response.batch_id
            total_tasks = bulk_response.total_tasks
            console.print(
                f"Batch submitted with ID: [bold cyan]{batch_id}[/bold cyan] containing "
            )
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

            # Format duration into hours, minutes, seconds with None handling
            duration = getattr(batch, "duration", 0) or 0
            hours = int(duration // 3600)
            minutes = int((duration % 3600) // 60)
            seconds = int(duration % 60)
            duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            # Safely get token values with fallback to 0
            total_tokens = getattr(batch, "total_tokens", 0) or 0
            prompt_tokens = getattr(batch, "prompt_tokens", 0) or 0
            completion_tokens = getattr(batch, "completion_tokens", 0) or 0

            # Calculate costs
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

        # Monitor progress
        with Live(render_dashboard(), refresh_per_second=2, console=console) as live:
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
        tasks_data = self.get_batch_tasks_all(batch_id)

        return tasks_data
