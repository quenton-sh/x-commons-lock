package x.commons.lock.distributed;

import java.util.Map;

import org.apache.zookeeper.ZooKeeper;

import x.commons.util.Provider;

public class ZooKeeperProvider implements Provider<ZooKeeper> {
	
	private final ZooKeeper zk;

	public ZooKeeperProvider(ZooKeeper zk) {
		this.zk = zk;
	}

	@Override
	public ZooKeeper get() {
		return this.zk;
	}

	@Override
	public ZooKeeper get(Object... args) {
		return this.zk;
	}

	@Override
	public ZooKeeper get(Map<String, Object> args) {
		return this.zk;
	}

}
