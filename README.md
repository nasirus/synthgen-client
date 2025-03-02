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
    timeout=3600  # Optional request timeout in seconds
)
```

### Configuration File

You can use a JSON configuration file for easier configuration management:

```python
# config.json
# {
#   "base_url": "http://localhost:8002",
#   "api_key": "your-api-key",
#   "timeout": 3600
# }

client = SynthgenClient(config_file="config.json")
```

The configuration is loaded in the following order of precedence:
1. Direct parameters passed to the constructor
2. Environment variables
3. Configuration file values

This allows for flexible configuration management across different environments.

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

## Performance Optimization Options

Two key parameters optimize task execution:

```python
Task(
    # Other parameters...
    use_cache=True,     # Use cached results when available (default: True)
    track_progress=True # Enable detailed progress tracking (default: True)
)
```

### Caching (`use_cache`)

Controls whether to use previously cached results:

- **True**: Reuses results for identical tasks, reducing API calls and costs
- **False**: Always executes fresh requests, ensuring up-to-date responses

```python
# Check if results came from cache
if task_result.cached:
    print("Retrieved from cache")
```

### Progress Tracking (`track_progress`)

Controls the level of execution monitoring:

- **True**: Provides detailed metrics (tokens, duration, status updates)
- **False**: Minimal tracking for improved performance

### Usage Examples

```python
# Optimize for speed with caching
task_cached = Task(custom_id="cached", use_cache=True, track_progress=False, ...)

# Ensure fresh results with metrics
task_fresh = Task(custom_id="fresh", use_cache=False, track_progress=True, ...)

# Mixed batch processing
results = client.monitor_batch(tasks=[task_cached, task_fresh])
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

## Monitoring Existing Batches

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

## Token Usage Tracking and Cost Calculation

The client provides detailed token usage statistics and cost calculation capabilities for batches:

```python
# Process a batch with cost tracking
results = client.monitor_batch(
    tasks=tasks,
    cost_by_1m_input_token=0.01,  # Cost per million input tokens
    cost_by_1m_output_token=0.03  # Cost per million output tokens
)

# Retrieve batch statistics
batch = client.get_batch(batch_id)
print(f"Input tokens: {batch.prompt_tokens:,}")
print(f"Output tokens: {batch.completion_tokens:,}")
print(f"Total tokens: {batch.total_tokens:,}")
```

This allows for real-time cost estimation and budget tracking when using pay-per-token LLM services.

## Resilient Error Handling and Auto-Retry

The client implements sophisticated error handling with automatic retries for transient network issues:

```python
# The client automatically handles retries with exponential backoff
# Max retries and other parameters are configurable
try:
    result = client.get_task("task-id")
except APIError as e:
    if e.status_code == 404:
        print("Task not found")
    elif e.status_code == 401:
        print("Authentication failed - check your API key")
    else:
        print(f"An error occurred: {str(e)}")
```

## Requirements

- Python 3.8+
- httpx>=0.24.0
- pydantic>=2.0.0
- rich (for progress displays)

## License

MIT