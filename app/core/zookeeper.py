from kazoo.client import KazooClient
import threading

class ZooKeeperTokenManager:
    """
    Implements range-based ID allocation using ZooKeeper 
    to prevent single points of contention and collisions.
    """
    def __init__(self, zk_hosts: str, range_size: int = 1000):
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()
        self.counter_path = "/url_shortener/counter"
        self.zk.ensure_path(self.counter_path)
        
        self.range_size = range_size
        self.lock = threading.Lock()
        
        # Local memory boundaries
        self.current_id = 0
        self.max_id = 0

    def _fetch_new_range(self):
        """Atomically increments the ZK counter and claims a new batch of IDs."""
        # Kazoo's Counter provides an atomic distributed counter
        zk_counter = self.zk.Counter(self.counter_path)
        zk_counter += 1
        batch_index = zk_counter.value
        
        # Calculate local range boundaries based on the batch index
        self.current_id = (batch_index - 1) * self.range_size
        self.max_id = (batch_index * self.range_size) - 1

    def get_next_id(self) -> int:
        """Returns the next unique numeric ID from the local range."""
        with self.lock:
            # If we've exhausted our local range, reach out to ZK for a new batch
            if self.current_id > self.max_id or self.current_id == 0:
                self._fetch_new_range()
            
            assigned_id = self.current_id
            self.current_id += 1
            return assigned_id