import os
import time
from dotenv import load_dotenv
from synthgen import SynthgenClient
from synthgen.models import TaskListSubmission, TaskSubmission

load_dotenv()
client = SynthgenClient(base_url="http://localhost:8000")

# message_id = client.create_task(TaskRequest(model="gpt-4o-mini", messages=[Message(role="user", content="Hello, how are you?")]))
print(client.check_health())

batch = client.create_batch_json(
    TaskListSubmission(
        tasks=[
            TaskSubmission(
                custom_id="test-1",
                method="POST",
                url="https://openrouter.ai/api/v1/chat/completions",
                api_key=os.getenv("OPENROUTER_API_KEY"),
                body={
                    "model": "meta-llama/llama-3.2-1b-instruct",
                    "messages": [
                        {"role": "system", "content": "You are a helpful assistant."},
                        {
                            "role": "user",
                            "content": "Write a short story about a robot learning to cook.",
                        },
                    ],
                    "max_tokens": 500,
                    "temperature": 0.7,
                    "stream": False,
                },
            )
        ]
    )
)

print(batch)

time.sleep(2)
print(client.get_batch(batch.batch_id))
print(client.get_batch_tasks_all(batch.batch_id))

