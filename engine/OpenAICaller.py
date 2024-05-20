from math import exp
import time
import os
import openai
from openai import OpenAI, BadRequestError
from dotenv import load_dotenv
import tiktoken

load_dotenv()

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
)

class OpenAICaller:
    def __init__(self):
        # Limits have to be manually set as API has no way of retrieving them.
        self.models = {
            'gpt-3.5': {"name": "gpt-3.5-turbo-16k", "limit": 16_385},
            'gpt-4': {"name": "gpt-4o", "limit": 128_000},
            'gpt-3.5-ft-SIExtraction': {"name": "ft:gpt-3.5-turbo-0125:personal::9AU7vXs3", "limit": 16_385},
        }

        self.model_default = self.models['gpt-3.5']


    def query(self, system, user, temp = 1, model = "gpt-3.5", n = 1):

        if model not in self.models:
            print(f"Model {model} not found, using default model")
            model = self.model_default
        
        selected_model = self.models[model]

        total_length = sum(self.get_tokens([system, user]))
        
        # This is hardcoded due to there being no API way of seeing if there are too many tokens.
        if total_length > selected_model['limit']:
            print(f"Too many tokens {total_length}, not sending to OpenAI")
            return None

        # If rate limit error happens then just wait a minute and try again
        try:
            completion = client.chat.completions.create(
            model = selected_model['name'],
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user}
            ],
            temperature=temp, n = n,
            )
        except openai.BadRequestError as e:
            print(f'Too many tokens in request OpenAI declined:\n {e}')
            return None

        if n == 1:
            return completion.choices[0].message.content
        else:
            return [choice.message.content for choice in completion.choices]
        
    def get_tokens(self, texts):
        # No need to check for model as all models have the same encoding
        enc = tiktoken.encoding_for_model("gpt-3.5-turbo-16k")
        return [len(enc.encode(text)) for text in texts]
        
            
openAICaller = OpenAICaller()

