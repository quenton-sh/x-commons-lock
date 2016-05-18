package x.commons.lock.distributed;

import org.apache.zookeeper.ZooKeeper;

import x.commons.lock.LockException;
import x.commons.lock.SimpleLock;
import x.commons.util.Provider;


public class ZooKeeperLockPool extends AbstractLockPool {
	
	private final Provider<ZooKeeper> zkProvider;
	private final String nodePath;

	public ZooKeeperLockPool(ZooKeeper zk, String nodePath, int size) {
		this(new ZooKeeperProvider(zk), nodePath, size);
	}
	
	public ZooKeeperLockPool(Provider<ZooKeeper> zkProvider, String nodePath, int size) {
		super(size);
		this.zkProvider = zkProvider;
		this.nodePath = nodePath;
	}

	@Override
	protected SimpleLock newLock(String key) throws LockException {
		return new ZooKeeperLock(this.zkProvider, this.nodePath, key);
	}

}
