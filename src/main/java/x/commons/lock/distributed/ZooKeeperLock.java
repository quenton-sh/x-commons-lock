package x.commons.lock.distributed;

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import x.commons.lock.LockException;

public class ZooKeeperLock extends AbstractReentrantLock {

	private final ZooKeeper zk;
	private final String node;
	private final Object mutex = new byte[0];
	
	private Long mySeq = null;

	public ZooKeeperLock(ZooKeeper zk, String nodePath) throws LockException {
		this(zk, nodePath, "__DEFAULT__");
	}
	
	public ZooKeeperLock(ZooKeeper zk, String nodePath, String key) throws LockException {
		try {
			this.zk = zk;
			
			StringBuilder sb = new StringBuilder(nodePath);
			if (sb.charAt(sb.length() - 1) != '/') {
				sb.append("/");
			}
			sb.append(key);
			this.node = sb.toString();
			
			this.createNodeForKey();
		} catch (Exception e) {
			if (e instanceof LockException) {
				throw (LockException) e;
			} else {
				throw new LockException(e);
			}
		}
	}
	
	private void createNodeForKey() throws Exception {
		Stat stat = zk.exists(this.node, null);
		if (stat == null) {
			String createdNode = zk.create(this.node, null,
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			logger.info(String.format("Zookeeper persistent node '%s' created.", createdNode));
		}
	}
	
	@Override
	protected void lockGlobal() throws Exception {
		String createdNode = zk.create(this.buildPathForNewNode(), 
				null, 
				ZooDefs.Ids.OPEN_ACL_UNSAFE, 
				CreateMode.EPHEMERAL_SEQUENTIAL);
		logger.debug(String.format("Zookeeper ephemeral node '%s' created.", createdNode));
		
		synchronized (mutex) {
			if (!this.checkState()) {
				mutex.wait();
			}
			// 已获得全局锁
			logger.debug(String.format("Thread %d acquired the global lock.", Thread.currentThread().getId()));
		}
	}
	
	private final boolean checkState() throws Exception {
		String previousNode = this.getPreviousNodeNameInQueue();
		if (previousNode == null) {
			return true;
		} else {
			// 在序列前一个结点注册Watcher，等待...
			Stat stat = zk.exists(buildPathForNode(previousNode), new NodeWatcher());
			if (stat == null) {
				// 可能代码从 zk.getChildren 执行到当前行这个过程中，前面的结点被删除了，当前结点有可能变成最大了，所以重新检查一遍
				logger.debug("The node before me may be removed. Retry.");
				return checkState();
			}
			return false;
		}
	}
	
	private String getPreviousNodeNameInQueue() throws Exception {
		List<String> childrenNames = zk.getChildren(this.node, false);
		if (childrenNames == null || childrenNames.size() == 0) {
			throw new LockException(String.format("No child found for node '%s'.", this.node));
		}
		
		TreeMap<Long, String> treeMap = new TreeMap<Long, String>();
		mySeq = null;
		for (String name : childrenNames) {
			long[] ll = this.parseSessionIdAndSeqForNode(name);
			long sessionId = ll[0];
			long seq = ll[1];
			treeMap.put(seq, name);
			if (sessionId == zk.getSessionId()) {
				mySeq = seq;
			}
		}
		if (mySeq == null) {
			throw new LockException(
					String.format("Child created for node '%s' but can't be found.", this.node));
		}
		Entry<Long, String> previous = treeMap.lowerEntry(mySeq);
		if (previous == null) {
			return null;
		} else {
			return previous.getValue();
		}
	}

	@Override
	protected void unlockGlobal() throws Exception {
		if (mySeq == null) {
			throw new LockException("Illegal state: can't get seq for the node to be removed.");
		}
		zk.delete(this.buildPathForNode(this.mySeq), -1);
	}
	
	private String buildPathForNewNode() {
		return String.format("%s/session-%d-", this.node, zk.getSessionId());
	}
	
	private String buildPathForNode(long seq) {
		return String.format("%s/session-%d-%010d", this.node, zk.getSessionId(), seq);
	}
	
	private String buildPathForNode(String name) {
		return String.format("%s/%s", this.node, name);
	}
	
	private long[] parseSessionIdAndSeqForNode(String path) {
		String[] ss = path.split("-");
		return new long[] {Long.parseLong(ss[ss.length - 2]), Long.parseLong(ss[ss.length - 1])};
	}

	private class NodeWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			logger.debug(String.format(
					"Event fired on path: '%s', state: '%s', type: '%s'.",
					event.getPath(), event.getState().toString(), event.getType().toString()));
			if (event.getType() == org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted) {
				try {
					synchronized(mutex) {
						if (checkState()) {
							// 已获得全局锁
							mutex.notify();
						}
					}
				} catch (Exception e) {
					logger.error(e.toString(), e);
				}
			}
		}

	}
	
}
