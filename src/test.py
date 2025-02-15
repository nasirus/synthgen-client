import os
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd
from synthgen.sync_client import SynthgenClient
from synthgen.models import Task

load_dotenv()
# logger = logging.getLogger(__name__)


client = SynthgenClient(base_url="http://localhost:8002")

print("Loading dataset...")
dataset_name = "nvidia/AceMath-Instruct-Training-Data"
dataset = load_dataset(dataset_name)


# Convert to list or iterate properly
data = list(dataset["math_sft"].select(range(1)))  # Convert to list first

print("Creating tasks...")
tasks = [
    Task(
        custom_id=f"id-{idx}",
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
        use_cache=True,
        track_progress=True,
    )
    for idx, row in enumerate(data)
]


batch = client.monitor_batch(
    tasks=tasks,
    cost_by_1m_input_token=0.005,
    cost_by_1m_output_token=0.01,
)


print("Saving to parquet...")
# Save to parquet using pandas

df = batch.to_dataframe()
df.to_parquet("batch_results.parquet")

print("Loading and displaying the saved parquet file...")
# Load and display the saved parquet file
loaded_df = pd.read_parquet("batch_results.parquet")
print("\nFirst 3 rows of loaded parquet file:")
print(loaded_df.head(5))
