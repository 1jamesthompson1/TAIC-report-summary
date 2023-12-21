from math import exp
import time
import os
from openai import OpenAI
from dotenv import load_dotenv
import tiktoken

load_dotenv()

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
)

class OpenAICaller:
    def __init__(self):
        self.model = "gpt-3.5-turbo-16k"
        
        self.model_large = "gpt-4-1106-preview"
    def setup(self):
        # Set up the OpenAI API credentials and other configuration options
        pass

    def query(self, system, user, temp = 1, large_model = False, n = 1):

        model= self.model_large if large_model else self.model

        # Check to make sure there arent too many tokens
        total_length = sum(self.get_tokens(model, [system, user]))+1
        
        # This is hardcoded due to there being no API way of seeing if there are too many tokens.
        if (large_model and (total_length > 128000)) or ( (not large_model) and (total_length > 16000)):
            print(f"Too many tokens {total_length}, not sending to OpenAI")
            return None

        # If rate limit error happens then just wait a minute and try again
        completion = client.chat.completions.create(
        model = model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ],
        temperature=temp, n = n,
        max_tokens=2000,
        )

        if n == 1:
            return completion.choices[0].message.content
        else:
            return [choice.message.content for choice in completion.choices]
        
    def get_tokens(self, model, texts):
        enc = tiktoken.encoding_for_model(model)
        return [len(enc.encode(text)) for text in texts]
        
            
openAICaller = OpenAICaller()

