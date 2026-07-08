import logging
import pytest
import redis
from app.core.redis import redis_safe_get, redis_safe_set


def test_redis_safe_get_logs_warning_on_redis_error(mocker, caplog):
    mock_client = mocker.MagicMock()
    mock_client.get.side_effect = redis.RedisError("Connection refused")

    with caplog.at_level(logging.WARNING, logger="app.core.redis"):
        result = redis_safe_get(mock_client, "test_key")

    assert result is None
    assert "Redis get failed for key 'test_key'" in caplog.text
    assert "Connection refused" in caplog.text


def test_redis_safe_set_logs_warning_on_redis_error(mocker, caplog):
    mock_client = mocker.MagicMock()
    mock_client.set.side_effect = redis.RedisError("OOM command not allowed")

    with caplog.at_level(logging.WARNING, logger="app.core.redis"):
        redis_safe_set(mock_client, "test_key", "http://example.com", ex=60)

    assert "Redis set failed for key 'test_key'" in caplog.text
    assert "OOM command not allowed" in caplog.text
