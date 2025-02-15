import os
import requests
from dotenv import load_dotenv

load_dotenv()


def call_openai_api(prompt, system_message="You are a helpful assistant."):
    # API endpoint
    url = "https://api.openai.com/v1/chat/completions"

    # Get API key from environment variable
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")

    # Headers
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}

    # Request body
    data = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_message},
            {"role": "user", "content": prompt},
        ],
    }

    # Make the API call
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Parse and return the response
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making API call: {e}")
        return None


# Example usage
if __name__ == "__main__":
    prompt = "Write a haiku that explains the concept of recursion."
    response = call_openai_api(prompt)

    if response:
        # Extract and print the assistant's response
        try:
            assistant_message = response["choices"][0]["message"]["content"]
            # print("Response:", assistant_message)
            print("Response:", response)
        except KeyError as e:
            print(f"Error parsing response: {e}")
            print("Full response:", response)
