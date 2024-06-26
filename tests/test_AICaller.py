import pytest

from engine.utils.AICaller import AICaller


@pytest.mark.parametrize(
    "model, user, temp, n, expected_response",
    [
        pytest.param("gpt-3.5", "Hello this is a test", 0, 1, True, id="gpt-3.5"),
        pytest.param("gpt-4", "Hello this is a test", 0, 1, True, id="gpt-4"),
        pytest.param(
            "gpt-4",
            "Hello this is a test" * (10**6),
            0,
            1,
            False,
            id="gpt-4 over limit",
        ),
        pytest.param(
            "gpt-4",
            "Testing multiple responses with changing n",
            0,
            2,
            True,
            id="gpt-4 multiple responses",
        ),
        pytest.param(
            "claude-3.5-sonnet", "Hello this is a test", 0, 1, True, id="claude-3.5"
        ),
        pytest.param(
            "claude-3.5-sonnet",
            "No n support",
            0,
            2,
            False,
            id="claude-3.5 multiple responses",
        ),
        pytest.param(
            "claude-3.5-sonnet",
            "Testing over the limit." * (10**6),
            0,
            1,
            False,
            id="claude-3.5 over limit",
        ),
    ],
)
def test_ai_caller(model, user, temp, n, expected_response):
    response = AICaller.query(
        model=model, system="", user=user, temp=temp, n=n, max_tokens=100
    )

    print(f"Response: '{response}'")
    if n == 1:
        assert isinstance(response, str) == expected_response
        return
    elif not expected_response:
        assert response is None

    assert isinstance(response, list)
    assert len(response) == n

    assert all([isinstance(res, str) for res in response])
