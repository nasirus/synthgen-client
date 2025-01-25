from synthgen import SynthgenClient

client = SynthgenClient(base_url="http://localhost:8000", api_key="test-api-key")

task = client.get_task(message_id="de7c62ac-7e98-4783-b5b0-ebb85ffed5f0")
print(task.batch_id)

batch = client.get_batch(batch_id=task.batch_id)
print(batch)



