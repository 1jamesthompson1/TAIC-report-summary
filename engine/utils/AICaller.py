import os

import anthropic
import openai
import tiktoken
from dotenv import load_dotenv

load_dotenv()

openai_client = openai.OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
)

anthropic_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


class BaseAICaller:
    def __init__(self, client, model, limit):
        self.client = client
        self.model = model
        self.limit = limit

    def get_tokens(self, texts):
        # No need to check for model as all models have the same encoding
        enc = tiktoken.encoding_for_model("gpt-3.5-turbo-16k")
        return [len(enc.encode(text)) for text in texts]

    def check_query_above_limit(self, query):
        return sum(self.get_tokens([query])) > self.limit


class OpenAICaller(BaseAICaller):
    def __init__(self, client, model, limit):
        super().__init__(client, model, limit)

    def query(self, system, user, temp, n, max_tokens=1024):
        if self.check_query_above_limit(system + user):
            print("Too many tokens, not sending to OpenAI")
            return None

        # If rate limit error happens then just wait a minute and try again
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                temperature=temp,
                n=n,
                max_tokens=max_tokens,
                seed=42,
            )
        except openai.BadRequestError as e:
            print(f"Too many tokens in request OpenAI declined:\n {e}")
            return None

        if n == 1:
            return completion.choices[0].message.content
        else:
            return [choice.message.content for choice in completion.choices]


class AnthropicCaller(BaseAICaller):
    def __init__(self, client, model, limit):
        super().__init__(client, model, limit)

    def query(self, system, user, temp, n=1, max_tokens=1024):
        if n != 1:
            raise ValueError("Anthropic only supports n=1")

        if self.check_query_above_limit(system + user):
            print("Too many tokens, not sending to Anthropic")
            return None

        completion = self.client.messages.create(
            model=self.model,
            system=system,
            messages=[{"role": "user", "content": user}],
            temperature=temp,
            max_tokens=max_tokens,
        )

        return completion.content[0].text


class AICaller:
    def __init__(self):
        self.models = {
            "gpt-3.5": OpenAICaller(openai_client, "gpt-3.5-turbo-16k", 16_385),
            "gpt-4": OpenAICaller(openai_client, "gpt-4o", 128_000),
            "gpt-4o-mini": OpenAICaller(openai_client, "gpt-4o-mini", 128_000),
            "gpt-3.5-ft-SIExtraction": OpenAICaller(
                openai_client, "ft:gpt-3.5-turbo-0125:personal::9AU7vXs3", 16_385
            ),
            "claude-3.5-sonnet": AnthropicCaller(
                anthropic_client, "claude-3-5-sonnet-20240620", 200_000
            ),
        }

        self.model_default = self.models["gpt-3.5"]

    def query(self, system, user, temp=1, model="gpt-3.5", n=1, max_tokens=1024):
        if model not in self.models:
            raise ValueError(
                f"Model {model} not found. Available models: {list(self.models.keys())}"
            )

        selected_model = self.models[model]

        return selected_model.query(system, user, temp, n, max_tokens)


AICaller = AICaller()
