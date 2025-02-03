import httpx
import json
from typing import Optional, Dict, Any, List, Union, BinaryIO
from pathlib import Path
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


class SynthgenClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: int = 30):
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
        """Make an HTTP request to the API"""
        url = f"{self.base_url}{path}"

        try:
            response = self._client.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json() if response.content else None
        except httpx.HTTPStatusError as e:
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

    def create_batch(self, file_path: Union[str, Path, BinaryIO]) -> Dict[str, Any]:
        """Create a new batch from a JSONL file"""
        if isinstance(file_path, (str, Path)):
            files = {"file": open(file_path, "rb")}
        else:
            files = {"file": file_path}

        response = self._request("POST", "/api/v1/batches", files=files)
        return response

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

    def create_batch_json(self, tasks: TaskListSubmission) -> BulkTaskResponse:
        """
        Submit bulk tasks using a JSON payload instead of a file.

        Args:
            tasks: TaskListSubmission object containing a list of TaskSubmission objects
                  Each TaskSubmission contains:
                  - custom_id: Unique identifier for the task
                  - method: HTTP method
                  - url: Target URL
                  - api_key: Optional API key
                  - body: Request body as dictionary

        Returns:
            A dictionary representing the BulkTaskResponse with keys:
                - batch_id: Identifier for the created batch
                - rows: Count of tasks submitted

        Raises:
            APIError: If an error occurs during JSON conversion or during the API request
        """
        try:
            # Convert the TaskListSubmission to a JSON string
            json_payload = tasks.model_dump_json()
        except Exception as e:
            raise APIError(f"Error converting tasks to JSON string: {e}")

        # Prepare headers by including the appropriate Content-Type
        headers = self._get_headers()
        headers["Content-Type"] = "application/json"

        response = self._request(
            "POST", "/api/v1/batches/json", content=json_payload, headers=headers
        )
        return BulkTaskResponse.model_validate(response)
