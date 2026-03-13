"""Optional LLM integration tests.

These tests hit a real model/provider via `litellm`.

They are skipped unless the following environment variables are set (typically
via a local `.env` file):

- ETL_DECORATORS_TESTS_LLM_MODEL
- ETL_DECORATORS_TESTS_LLM_API_KEY
"""

import pytest


# litellm currently triggers a DeprecationWarning on Python 3.12+ by calling
# `asyncio.get_event_loop()` when no event loop is set.
pytestmark = pytest.mark.filterwarnings(
    "ignore:There is no current event loop:DeprecationWarning:litellm\\..*"
)
from pydantic import BaseModel


def test_llm_integration_text(llm):
    @llm
    def ping() -> str:
        return "Reply with exactly: pong"

    out = ping()
    assert isinstance(out, str)
    assert out.strip().lower() == "pong"

def test_llm_integration_structured(llm):

    class Response(BaseModel):
        city: str
        country: str
        is_capital: bool

    @llm(return_type=Response)
    def ping(description: str) -> Response:
        return "Which city are we talking about? Which country is it in? Is it a capital city? " \
            "Respond with a JSON object with keys: city, country, is_capital (boolean). " \
            "\n\n---\n\n" \
            f"Description: {description}"

    dataset = [
        ("The city of lights, known for the Eiffel Tower.", Response(city="Paris", country="France", is_capital=True)),
        ("This city has been split in 4 after WW2.", Response(city="Berlin", country="Germany", is_capital=True)),
        ("A city in Japan famous for its cherry blossoms and temples.", Response(city="Kyoto", country="Japan", is_capital=False)),
    ]
    for description, expected in dataset:
        response = ping(description)
        print(response)
        assert isinstance(response, Response)
        assert response == expected