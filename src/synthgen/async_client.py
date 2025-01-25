import httpx
import json
from typing import Optional, Dict, Any, List, Union, BinaryIO, AsyncGenerator
from pathlib import Path
from .models import (
    Task, Batch, BatchList, TaskList,
    TaskRequest, Message, HealthResponse
)
from .exceptions import APIError

class AsyncSynthgenClient:
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: int = 30
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers=self._get_headers()
        )

    def _get_headers(self) -> Dict[str, str]:
        headers = {
            'Accept': 'application/json',
        }
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
        return headers

    async def close(self):
        """Close the underlying HTTP client"""
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs
    ) -> Any:
        """Make an HTTP request to the API"""
        url = f"{self.base_url}{path}"
        
        try:
            response = await self._client.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json() if response.content else None
        except httpx.HTTPStatusError as e:
            raise APIError(
                str(e),
                status_code=e.response.status_code,
                response=e.response
            )
        except Exception as e:
            raise APIError(str(e))

    async def create_task(self, model: str, messages: List[Dict[str, str]]) -> str:
        """Create a new task"""
        task_request = TaskRequest(
            model=model,
            messages=[Message(**msg) for msg in messages]
        )
        response = await self._request("POST", "/api/v1/tasks", json=task_request.model_dump())
        return response["message_id"]

    async def get_task(self, message_id: str) -> Task:
        """Get task status and result"""
        response = await self._request("GET", f"/api/v1/tasks/{message_id}")
        return Task.model_validate(response)


    async def delete_task(self, message_id: str) -> None:
        """Delete a task"""
        await self._request("DELETE", f"/api/v1/tasks/{message_id}")

    async def create_batch(self, file_path: Union[str, Path, BinaryIO]) -> Dict[str, Any]:
        """Create a new batch from a JSONL file"""
        if isinstance(file_path, (str, Path)):
            files = {'file': open(file_path, 'rb')}
        else:
            files = {'file': file_path}
        
        response = await self._request("POST", "/api/v1/batches", files=files)
        return response

    async def get_batch_status(self, batch_id: str) -> Batch:
        """Get batch status"""
        response = await self._request("GET", f"/api/v1/batches/{batch_id}")
        return Batch.model_validate(response)

    async def list_batches(
        self,
        page: int = 1,
        page_size: int = 50
    ) -> BatchList:
        """List all batches"""
        response = await self._request(
            "GET",
            "/api/v1/batches",
            params={"page": page, "page_size": page_size}
        )
        return BatchList.model_validate(response)

    async def get_batch_tasks(
        self,
        batch_id: str,
        page: int = 1,
        page_size: int = 50
    ) -> TaskList:
        """Get tasks in a batch"""
        response = await self._request(
            "GET",
            f"/api/v1/batches/{batch_id}/tasks",
            params={"page": page, "page_size": page_size}
        )
        return TaskList.model_validate(response)

    async def export_batch(
        self,
        batch_id: str,
        format: str = "json",
        chunk_size: int = 1000,
        include_fields: Optional[List[str]] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Export batch data as a stream"""
        params = {
            "format": format,
            "chunk_size": chunk_size
        }
        if include_fields:
            params["include_fields"] = ",".join(include_fields)

        async with self._client.stream(
            "GET",
            f"{self.base_url}/api/v1/batches/{batch_id}/export",
            params=params
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.strip():
                    yield json.loads(line.rstrip(","))

    async def delete_batch(self, batch_id: str) -> None:
        """Delete a batch"""
        await self._request("DELETE", f"/batches/{batch_id}")

    async def check_health(self) -> HealthResponse:
        """Check system health status"""
        response = await self._request("GET", "/health")
        return HealthResponse.model_validate(response) 