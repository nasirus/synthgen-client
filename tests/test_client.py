import pytest
import httpx
from synthgen.async_client import SyntheticDataClient
from synthetic_data_client.models import TaskStatus, TaskResponse, BatchStatus, BatchList, BatchTaskList
from synthetic_data_client.exceptions import APIError

@pytest.fixture
def client():
    return SyntheticDataClient(
        base_url="http://test-api.example.com",
        api_key="test-api-key"
    )

@pytest.fixture
def mock_response():
    return {
        "message_id": "test-message-id",
        "status": "completed",
        "payload": {"prompt": "test prompt"},
        "result": {"response": "test response"},
        "created_at": "2024-03-20T10:00:00Z",
        "completed_at": "2024-03-20T10:01:00Z",
        "cached": False,
        "total_tokens": 100
    }

@pytest.mark.asyncio
async def test_create_task(client):
    async with httpx.AsyncClient() as mock_client:
        mock_client.post = lambda *args, **kwargs: httpx.Response(
            200,
            json={"message_id": "test-message-id"}
        )
        client._client = mock_client
        
        messages = [{"role": "user", "content": "Hello"}]
        message_id = await client.create_task("gpt-3.5-turbo", messages)
        
        assert message_id == "test-message-id"

@pytest.mark.asyncio
async def test_get_task(client, mock_response):
    async with httpx.AsyncClient() as mock_client:
        mock_client.get = lambda *args, **kwargs: httpx.Response(
            200,
            json=mock_response
        )
        client._client = mock_client
        
        response = await client.get_task("test-message-id")
        
        assert isinstance(response, TaskResponse)
        assert response.message_id == "test-message-id"
        assert response.status == TaskStatus.COMPLETED

@pytest.mark.asyncio
async def test_delete_task(client):
    async with httpx.AsyncClient() as mock_client:
        mock_client.delete = lambda *args, **kwargs: httpx.Response(204)
        client._client = mock_client
        
        await client.delete_task("test-message-id")

@pytest.mark.asyncio
async def test_create_batch(client, tmp_path):
    # Create a temporary JSONL file
    test_file = tmp_path / "test.jsonl"
    test_file.write_text('{"test": "data"}\n')
    
    async with httpx.AsyncClient() as mock_client:
        mock_client.post = lambda *args, **kwargs: httpx.Response(
            200,
            json={"batch_id": "test-batch-id"}
        )
        client._client = mock_client
        
        response = await client.create_batch(test_file)
        assert response["batch_id"] == "test-batch-id"

@pytest.mark.asyncio
async def test_get_batch_status(client):
    batch_status_data = {
        "batch_id": "test-batch-id",
        "batch_status": "completed",
        "total_tasks": 10,
        "completed_tasks": 10,
        "failed_tasks": 0,
        "pending_tasks": 0,
        "cached_tasks": 2,
        "created_at": "2024-03-20T10:00:00Z",
        "completed_at": "2024-03-20T10:01:00Z",
        "total_tokens": 1000,
        "prompt_tokens": 500,
        "completion_tokens": 500
    }
    
    async with httpx.AsyncClient() as mock_client:
        mock_client.get = lambda *args, **kwargs: httpx.Response(
            200,
            json=batch_status_data
        )
        client._client = mock_client
        
        response = await client.get_batch_status("test-batch-id")
        assert isinstance(response, BatchStatus)
        assert response.batch_id == "test-batch-id"
        assert response.batch_status == TaskStatus.COMPLETED

@pytest.mark.asyncio
async def test_list_batches(client):
    batches_data = {
        "total": 1,
        "page": 1,
        "page_size": 50,
        "batches": [{
            "batch_id": "test-batch-id",
            "batch_status": "completed",
            "total_tasks": 10,
            "completed_tasks": 10,
            "failed_tasks": 0,
            "pending_tasks": 0,
            "cached_tasks": 2,
            "created_at": "2024-03-20T10:00:00Z",
            "total_tokens": 1000,
            "prompt_tokens": 500,
            "completion_tokens": 500
        }]
    }
    
    async with httpx.AsyncClient() as mock_client:
        mock_client.get = lambda *args, **kwargs: httpx.Response(
            200,
            json=batches_data
        )
        client._client = mock_client
        
        response = await client.list_batches()
        assert isinstance(response, BatchList)
        assert len(response.batches) == 1
        assert response.total == 1

@pytest.mark.asyncio
async def test_get_batch_tasks(client):
    tasks_data = {
        "total": 1,
        "page": 1,
        "page_size": 50,
        "tasks": [{
            "message_id": "test-message-id",
            "status": "completed",
            "cached": False,
            "payload": {"prompt": "test"},
            "result": {"response": "test"},
            "created_at": "2024-03-20T10:00:00Z",
            "completed_at": "2024-03-20T10:01:00Z",
            "total_tokens": 100
        }]
    }
    
    async with httpx.AsyncClient() as mock_client:
        mock_client.get = lambda *args, **kwargs: httpx.Response(
            200,
            json=tasks_data
        )
        client._client = mock_client
        
        response = await client.get_batch_tasks("test-batch-id")
        assert isinstance(response, BatchTaskList)
        assert len(response.tasks) == 1

@pytest.mark.asyncio
async def test_export_batch(client):
    export_data = '{"id": 1}\n{"id": 2}\n'
    
    async with httpx.AsyncClient() as mock_client:
        mock_client.stream = lambda *args, **kwargs: httpx.Response(
            200,
            text=export_data
        )
        client._client = mock_client
        
        items = []
        async for item in client.export_batch("test-batch-id"):
            items.append(item)
        
        assert len(items) == 2
        assert items[0]["id"] == 1
        assert items[1]["id"] == 2

@pytest.mark.asyncio
async def test_delete_batch(client):
    async with httpx.AsyncClient() as mock_client:
        mock_client.delete = lambda *args, **kwargs: httpx.Response(204)
        client._client = mock_client
        
        await client.delete_batch("test-batch-id")

@pytest.mark.asyncio
async def test_api_error(client):
    async with httpx.AsyncClient() as mock_client:
        mock_client.get = lambda *args, **kwargs: httpx.Response(
            404,
            json={"error": "Not found"}
        )
        client._client = mock_client
        
        with pytest.raises(APIError) as exc_info:
            await client.get_task("non-existent-id")
        
        assert exc_info.value.status_code == 404

@pytest.mark.asyncio
async def test_context_manager(client):
    async with SyntheticDataClient("http://test-api.example.com") as client:
        assert not client._client.is_closed
    assert client._client.is_closed 