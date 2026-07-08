from datetime import datetime, timezone, timedelta
from sqlmodel import select
from app.models.url_models import URLs
import base62


def test_shorten_url_success(client, session):
    expires = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
    payload = {
        "long_url": "https://www.google.com/search?q=fastapi",
        "expires_at": expires,
    }

    response = client.post("/api/v1/shorten", json=payload)
    assert response.status_code == 201

    data = response.json()
    assert data["long_url"] == payload["long_url"]
    assert "short_code" in data
    assert data["short_code"] == base62.encode(1001)
    assert data["short_url"] == f"http://localhost:8000/api/v1/{data['short_code']}"

    # Verify that the URL entry was saved to the database
    statement = select(URLs).where(URLs.short_code == data["short_code"])
    db_entry = session.exec(statement).first()
    assert db_entry is not None
    assert db_entry.long_text == payload["long_url"]


def test_shorten_url_missing_fields(client):
    payload = {"long_url": "https://www.google.com"}
    response = client.post("/api/v1/shorten", json=payload)
    assert response.status_code == 422  # Validation error for missing expires_at
