import os
import openai
from dotenv import load_dotenv

load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")

class OpenAICaller:
    def __init__(self):
        self.model = "gpt-3.5-turbo"
        self.model_large = "gpt-3.5-turbo-16k"
    def setup(self):
        # Set up the OpenAI API credentials and other configuration options
        pass

    def query(self, system, user, temp = 1, large_model = False, n = 1):

        model= self.model_large if large_model else self.model

        # Check to make sure there arent too many tokens
        enc = tiktoken.encoding_for_model(model)
        user_enc = enc.encode(user)
        system_enc = enc.encode(system)
        total_length = len(user_enc) + len(system_enc) + 1
        
        # This is hardcoded due to there being no API way of seeing if there are too many tokens.
        if (large_model & total_length > 16000) | ( (not large_model) & total_length > 4000):
            print("Too many tokens, not sending to OpenAI")
            return None

        if n == 1:
            return completion.choices[0].message.content
        else:
            return [choice.message.content for choice in completion.choices]
    
openAICaller = OpenAICaller()