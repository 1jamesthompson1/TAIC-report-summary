import os
import openai
from dotenv import load_dotenv

load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")

class OpenAICaller:
    def __init__(self):
        self.model = "gpt-3.5-turbo"

    def setup(self):
        # Set up the OpenAI API credentials and other configuration options
        pass

    def query(self, system, user):
        completion = openai.ChatCompletion.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user}
            ]
        )

        return completion.choices[0].message.content
    
openAICaller = OpenAICaller()