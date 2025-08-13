# This file call the llm(OpenAI)
import os
import json
import openai
from dotenv import load_dotenv

load_dotenv()


openai.api_key = os.getenv("OPENAI_API_KEY")

class LLMCaller:
    def __init__(self, model="gpt-4o"):  # Default model; change if needed
        self.model = model

    def llm_call(self, prompt):
        try:
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.0
            )
            return response.choices[0].message['content']
        except Exception as e:
            print(f"Error during OpenAI API call: {e}")
            return None
