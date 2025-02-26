# Synthetic Data Client

A Python client library for interacting with the Synthetic Data Generation API Framework Synthgen https://github.com/nasirus/synthgen.

## Installation

```bash
pip install synthgen-client
```

## Features

- Async/await support
- Type hints and validation using Pydantic
- Comprehensive error handling
- Streaming support for large exports
- Batch operations support
- Rich CLI progress displays
- Token usage and cost tracking

## Quick Start

```python
from synthgen import SynthgenClient
from synthgen.models import Task

# Initialize the client
client = SynthgenClient(
    base_url="https://api.synthgen.example.com",
    api_key="your-api-key"
)


# Example of a task using a local LLM provider
provider = "http://host.docker.internal:11434/v1/chat/completions"
model = "qwen2.5:0.5b"
api_key = "api_key"

# Create a single task
task = Task(
    custom_id="test",
    method="POST",
    url=provider,
    api_key=api_key,
    body={
        "model": model,
        "messages": [{"role": "user", "content": "solve 2x + 4 = 10"}],
    },
)

# Create a batch of tasks
tasks = [task]
for i in range(1, 10):
    tasks.append(Task(
        custom_id=f"task-00{i+1}",
        method="POST",
        url=provider,
        api_key=api_key,
        body={
            "model": model,
            "messages": [{"role": "user", "content": f"solve {i}x + 4 = 10"}],
        }
        )
    )

# Submit and monitor batch processing with cost tracking
results = client.monitor_batch(
    tasks=tasks,
    cost_by_1m_input_token=0.01,
    cost_by_1m_output_token=0.03
)

# Process results
for result in results:
    print(f"Task {result.message_id}: {result.status}")
    if result.body:
        print(f"Generated {len(result.body.get('data', []))} records")
```

## Configuration

The client can be configured in multiple ways:

### Environment Variables

```bash
# Set these environment variables
export SYNTHGEN_BASE_URL="http://localhost:8002"
export SYNTHGEN_API_KEY="your-api-key"

# Then initialize without parameters
client = SynthgenClient()
```

### Direct Parameters

```python
client = SynthgenClient(
    base_url="http://localhost:8002",
    api_key="your-api-key",
    timeout=7200  # 2 hours
)
```

### Configuration File

```python
# config.json
# {
#   "base_url": "http://localhost:8002",
#   "api_key": "your-api-key",
#   "timeout": 7200
# }

client = SynthgenClient(config_file="config.json")
```

## Batch Processing

The library provides powerful batch processing capabilities:

```python
# Create a batch of tasks
tasks = [
    Task(
        custom_id="task-001",
        method="POST",
        url=provider,
        api_key=api_key,
        body={
            "model": model,
            "messages": [{"role": "user", "content": "solve 2x + 4 = 10"}],
        },
        dataset="customers",
        use_cache=True,
    ),
    # Add more tasks...
]

# Submit batch and get batch_id
response = client.create_batch(tasks)
batch_id = response.batch_id

# Monitor batch progress with rich UI
results = client.monitor_batch(batch_id=batch_id)

# Or submit and monitor in one step
results = client.monitor_batch(tasks=tasks)
```

## Health Checks

```python
# Check system health
health = client.check_health()
print(f"System status: {health.status}")
print(f"API: {health.services.api}")
print(f"RabbitMQ: {health.services.rabbitmq}")
print(f"Elasticsearch: {health.services.elasticsearch}")
print(f"Queue consumers: {health.services.queue_consumers}")
```

## Task Management

```python
# Get task by ID
task = client.get_task("task-message-id")
print(f"Task status: {task.status}")
print(f"Completion time: {task.completed_at}")

# Delete a task
client.delete_task("task-message-id")
```

## Batch Management

```python
# Get all batches
batches = client.get_batches()
print(f"Total batches: {batches.total}")

# Get specific batch
batch = client.get_batch("batch-id")
print(f"Completed tasks: {batch.completed_tasks}/{batch.total_tasks}")
print(f"Token usage: {batch.total_tokens}")

# Get all tasks in a batch
tasks = client.get_batch_tasks("batch-id")

# Get only failed tasks
from synthgen.models import TaskStatus
failed_tasks = client.get_batch_tasks("batch-id", task_status=TaskStatus.FAILED)

# Delete a batch
client.delete_batch("batch-id")
```

## Context Manager Support

The client supports the context manager protocol for automatic resource cleanup:

```python
with SynthgenClient() as client:
    health = client.check_health()
    # Client will be automatically closed when exiting the with block
```

## Error Handling

The client provides robust error handling with automatic retries:

```python
from synthgen.exceptions import APIError

try:
    result = client.get_task("non-existent-id")
except APIError as e:
    print(f"API Error: {e.message}")
    print(f"Status code: {e.status_code}")
    if e.response:
        print(f"Response: {e.response.text}")
```

## Models

The library uses Pydantic models for type validation and serialization:

- `Task`: Represents a task to be submitted
- `TaskResponse`: Contains task results and metadata
- `Batch`: Contains batch status and statistics
- `BatchList`: Paginated list of batches
- `HealthResponse`: System health information
- `TaskStatus`: Enum of possible task states (PENDING, PROCESSING, COMPLETED, FAILED)

## Advanced Usage

### Monitoring Existing Batches

```python
# Monitor an existing batch
results = client.monitor_batch(
    batch_id="existing-batch-id",
    cost_by_1m_input_token=0.01,
    cost_by_1m_output_token=0.03
)
```

### Customizing Batch Creation

```python
# Create batch with custom chunk size for large batches
response = client.create_batch(tasks, chunk_size=500)
```

## Requirements

- Python 3.8+
- httpx>=0.24.0
- pydantic>=2.0.0
- rich (for progress displays)

## License

MIT