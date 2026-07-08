from datetime import datetime, timezone, timedelta
from kazoo.recipe.queue import uuid
from app.models.url_models import URLs


def test_redirect_cache_miss_populates_redis(client, session, fake_redis):
    # Insert a URL directly into the DB without hitting cache first
    test_code = "dbOnly01"
    target_url = "https://example.com/from-db"
    entry = URLs(
        id=uuid.uuid4(),
        short_code=test_code,
        long_text=target_url,
        expires_at=datetime.now(timezone.utc) + timedelta(days=1),
    )
    session.add(entry)
    session.commit()

    # Ensure Redis cache does not have this key initially
    assert fake_redis.get(test_code) is None

    # Request the redirect without auto-following redirects so we can inspect status and header
    response = client.get(f"/api/v1/{test_code}", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == target_url

    # Verify that cache was populated on cache miss
    assert fake_redis.get(test_code) == target_url


def test_redirect_cache_hit(client, fake_redis):
    test_code = "cached01"
    target_url = "https://example.com/from-cache"
    fake_redis.set(test_code, target_url)

    # Request redirect when key exists in Redis
    response = client.get(f"/api/v1/{test_code}", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == target_url


def test_redirect_not_found(client, fake_redis):
    response = client.get("/api/v1/nonExistentCode", follow_redirects=False)
    assert response.status_code == 404
