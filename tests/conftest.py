import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from app.main import app
from app.db.session import get_db
import app.api.endpoints as endpoints


class FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None):
        self.store[key] = value
        return True

    def clear(self):
        self.store.clear()


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture(name="fake_redis")
def fake_redis_fixture(mocker):
    fake = FakeRedis()
    mocker.patch.object(endpoints, "redis_client", fake)
    return fake


@pytest.fixture(name="mock_zk")
def mock_zk_fixture(mocker):
    # Mock get_next_id to yield sequential IDs starting at 1001
    counter = {"val": 1000}
    def _get_next():
        counter["val"] += 1
        return counter["val"]
    
    mock_manager = MagicMock()
    mock_manager.get_next_id.side_effect = _get_next
    mocker.patch.object(endpoints, "zk_manager", mock_manager)
    return mock_manager


@pytest.fixture(name="client")
def client_fixture(session: Session, fake_redis, mock_zk):
    def get_db_override():
        yield session

    app.dependency_overrides[get_db] = get_db_override
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()
