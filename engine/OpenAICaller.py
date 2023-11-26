from math import exp
import os
import time
import openai
from dotenv import load_dotenv
import tiktoken


load_dotenv()


openai.api_key = os.getenv("OPENAI_API_KEY")

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
            print("Too many tokens, not sending to OpenAI")
            return None

        # If rate limit error happens then just wait a minute and try again
        wait_time = 1
        while True:
            try:
                completion = openai.ChatCompletion.create(
                model = model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user}
                ],
                temperature=temp, n = n
                )
                break
            except openai.error.RateLimitError:
                print(f"Rate limit error, waiting {exp(wait_time)/60} minutes and trying again")        
                time.sleep(exp(wait_time))
                wait_time += 1
                continue        

        if n == 1:
            return completion.choices[0].message.content
        else:
            return [choice.message.content for choice in completion.choices]
        
    def get_tokens(self, model, texts):
        enc = tiktoken.encoding_for_model(model)
        return [len(enc.encode(text)) for text in texts]
        
            
openAICaller = OpenAICaller()

