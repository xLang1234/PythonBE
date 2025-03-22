import asyncio
import aiohttp
import time
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Get API key from environment variable
API_KEY = os.environ.get('OPENROUTER_API_KEY')
if not API_KEY:
    raise ValueError("OPENROUTER_API_KEY environment variable is not set. Please set it before running this script.")

API_URL = 'https://openrouter.ai/api/v1/chat/completions'

# Define common headers for all requests
headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

# List of models to test
models = [
    "deepseek/deepseek-chat:free",
    "google/gemma-3-4b-it:free",
    "mistralai/mistral-7b-instruct:free",
    "anthropic/claude-3-haiku:free",
    "meta-llama/llama-3-8b-instruct:free",
    "qwen/qwq-32b:free"
]

# Default instruction to prepend to all prompts
default_instruction = "You are a helpful AI assistant. Answer the following question concisely and accurately: "

# Test prompt to send to all models
test_prompt = "What is the meaning of life?"

async def query_model(session, model_name, instruction, prompt):
    """Query a specific model with the given instruction and prompt asynchronously"""
    combined_prompt = f"{instruction}{prompt}"
    
    data = {
        "model": model_name,
        "messages": [{"role": "user", "content": combined_prompt}]
    }
    
    start_time = time.time()
    
    try:
        async with session.post(API_URL, json=data, headers=headers) as response:
            elapsed_time = time.time() - start_time
            
            if response.status == 200:
                response_json = await response.json()
                content = response_json['choices'][0]['message']['content']
                return {
                    "model": model_name,
                    "response": content,
                    "time_taken": elapsed_time,
                    "status": "success"
                }
            else:
                response_text = await response.text()
                return {
                    "model": model_name,
                    "response": f"Error: {response.status} - {response_text}",
                    "time_taken": elapsed_time,
                    "status": "error"
                }
    except Exception as e:
        elapsed_time = time.time() - start_time
        return {
            "model": model_name,
            "response": f"Exception: {str(e)}",
            "time_taken": elapsed_time,
            "status": "exception"
        }

async def test_all_models_async(models_list, instruction, prompt):
    """Test all models concurrently and return their responses"""
    async with aiohttp.ClientSession() as session:
        tasks = []
        for model in models_list:
            task = query_model(session, model, instruction, prompt)
            tasks.append(task)
        
        print(f"Started {len(tasks)} async requests at {datetime.now().strftime('%H:%M:%S')}")
        results = await asyncio.gather(*tasks)
        print(f"All requests completed at {datetime.now().strftime('%H:%M:%S')}")
        
        return results

def setup_env_file():
    """Create a .env file if it doesn't exist"""
    if not os.path.exists('.env'):
        api_key = input("Enter your OpenRouter API key: ")
        with open('.env', 'w') as f:
            f.write(f"OPENROUTER_API_KEY={api_key}")
        print("Created .env file with your API key")
        # Reload environment variables
        load_dotenv()
        global API_KEY
        API_KEY = os.environ.get('OPENROUTER_API_KEY')

async def main():
    # Check for API key and set up .env file if needed
    if not API_KEY:
        setup_env_file()
    
    # Get custom instruction or use default
    custom_instruction = input("Enter your instruction (or press Enter to use default): ")
    if not custom_instruction.strip():
        custom_instruction = default_instruction
    else:
        # If user doesn't end instruction with space, add one
        if not custom_instruction.endswith(" "):
            custom_instruction += " "
    
    # Get custom prompt or use default
    user_prompt = input("Enter your prompt (or press Enter to use default): ")
    if not user_prompt.strip():
        user_prompt = test_prompt
    
    print(f"\nInstruction: '{custom_instruction}'")
    print(f"Prompt: '{user_prompt}'")
    print(f"Testing {len(models)} models...\n")
    
    results = await test_all_models_async(models, custom_instruction, user_prompt)
    
    print(f"\nCompleted testing {len(models)} models")

if __name__ == "__main__":
    asyncio.run(main())
