from unittest.mock import MagicMock
from app.core.zookeeper import ZooKeeperTokenManager


def test_zookeeper_token_manager_range_fetching(mocker):
    # Mock KazooClient so it does not connect to a real ZooKeeper server
    mock_kazoo = mocker.patch("app.core.zookeeper.KazooClient")
    mock_instance = mock_kazoo.return_value

    # Mock the distributed counter inside Kazoo
    mock_counter = MagicMock()
    mock_counter.value = 1  # First range batch: batch_index = 1 -> range 0..9
    mock_counter.__iadd__.return_value = mock_counter
    mock_instance.Counter.return_value = mock_counter

    # Instantiate manager with range_size = 10
    manager = ZooKeeperTokenManager(zk_hosts="127.0.0.1:2181", range_size=10)
    
    # First call should trigger _fetch_new_range because current_id == 0 initially
    first_id = manager.get_next_id()
    assert first_id == 0
    assert manager.current_id == 1
    assert manager.max_id == 9
    mock_counter.__iadd__.assert_called_once_with(1)

    # Subsequent calls within range should not fetch a new range
    second_id = manager.get_next_id()
    assert second_id == 1
    assert manager.current_id == 2
