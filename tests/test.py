import os
from dotenv import load_dotenv
from synthgen import SynthgenClient
from synthgen.models import TaskListSubmission, TaskSubmission
from datasets import load_dataset
import pandas as pd

load_dotenv()

client = SynthgenClient(base_url="http://localhost:8002")

print("Loading dataset...")
dataset_name = "nvidia/AceMath-Instruct-Training-Data"
dataset = load_dataset(dataset_name)

# Convert to list or iterate properly
data = list(dataset["math_sft"].select(range(100000)))  # Convert to list first

print("Creating tasks...")
tasks = TaskListSubmission(
    tasks=[
        TaskSubmission(
            custom_id=f"math-{idx}",
            method="POST",
            url="https://api.studio.nebius.ai/v1/chat/completions",
            api_key=os.getenv("NEBIUS_API_KEY"),
            body={
                "model": "meta-llama/Llama-3.2-1B-Instruct",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a mathematics tutor. Provide clear, step-by-step solutions.",
                    },
                    {"role": "user", "content": row["messages"][0]["content"]},
                ],
                "max_tokens": 1000,
                "temperature": 0.2,
                "stream": False,
            },
            dataset=dataset_name,
            source=row,
        )
        for idx, row in enumerate(data)
    ]
)

print("Monitoring batch...")
batch = client.monitor_batch(
    tasks=tasks,
    cost_by_1m_input_token=0.005,
    cost_by_1m_output_token=0.01,
)

print("Converting batch results to a list of dictionaries...")
# Convert batch results to a list of dictionaries
batch_data = [item.model_dump() for item in batch]

print("Saving to parquet...")
# Save to parquet using pandas

df = pd.DataFrame(batch_data)
df.to_parquet("batch_results.parquet")

print("Loading and displaying the saved parquet file...")
# Load and display the saved parquet file
loaded_df = pd.read_parquet("batch_results.parquet")
print("\nFirst 3 rows of loaded parquet file:")
print(loaded_df.head(3))
