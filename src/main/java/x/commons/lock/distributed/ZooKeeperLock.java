package x.commons.lock.distributed;

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import x.commons.lock.LockException;
import x.commons.util.Provider;

/**
 * Zookeeper分布式锁
 * <p>公平锁</p>
 * <p>锁不会超时自动释放</p>
 * <p>尝试获取锁时等待超时则获取失败</p>
 * 
 * 
 * @NotThreadSafe
 * @author Quenton
 */
public class ZooKeeperLock extends AbstractReentrantLock {

	private final Provider<ZooKeeper> zkProvider;
	private final String node;
	
	private volatile Long mySeq = null;
	private volatile boolean isLocked = false;
	
	private final BlockingQueue<byte[]> queue = new ArrayBlockingQueue<byte[]>(1);
	
	
	public ZooKeeperLock(ZooKeeper zk, String nodePath, String key) throws LockException {
		this(new ZooKeeperProvider(zk), nodePath, key);
	}
	
	public ZooKeeperLock(Provider<ZooKeeper> ZooKeeperProvider, String nodePath, String key) throws LockException {
		try {
			this.zkProvider = ZooKeeperProvider;
			
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
		ZooKeeper zk = this.zkProvider.get();
		String[] pathSec = this.node.substring(1).split("\\/"); // 去除前缀"/"后再按"/"分割
		for (int i = 0; i < pathSec.length; i++) {
			StringBuilder sb = new StringBuilder();
			for (int j = 0; j <= i; j++) {
				sb.append("/" + pathSec[j]);
			}
			String path = sb.toString();
			Stat stat = zk.exists(path, null);
			if (stat == null) {
				String createdNode = zk.create(path, null,
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				logger.info("Zookeeper persistent node '{}' created.", createdNode);
			}
		}
	}
	
	@Override
	protected boolean lockGlobal(long maxWaitTimeMillis) throws Exception {
		if (isLocked) {
			throw new IllegalStateException("Already locked!");
		}
		
		ZooKeeper zk = this.zkProvider.get();
		long waitTimeMillis = maxWaitTimeMillis;
		long startTs = System.currentTimeMillis();
		String createdNode = zk.create(this.buildPathForNewNode(zk.getSessionId()), 
				null, 
				ZooDefs.Ids.OPEN_ACL_UNSAFE, 
				CreateMode.EPHEMERAL_SEQUENTIAL);
		logger.debug("Zookeeper ephemeral node '{}' created.", createdNode);
		if (waitTimeMillis > 0) {
			waitTimeMillis -= System.currentTimeMillis() - startTs;
			if (waitTimeMillis <= 0) {
				// 超时
				deleteNodeWithLogging(zk, createdNode);
				return false;
			}
		}
		
		startTs = System.currentTimeMillis();
		this.queue.clear(); // 清空queue，以防前一次获取锁超时退出后，watcher收到事件通知仍向queue里写入，造成queue不为空
		if (!this.ifNotFirstThenAddWatcher(zk)) {
			if (waitTimeMillis > 0) {
				waitTimeMillis -= System.currentTimeMillis() - startTs;
				if (waitTimeMillis <= 0 || queue.poll(waitTimeMillis, TimeUnit.MILLISECONDS) == null) {
					// 超时
					deleteNodeWithLogging(zk, createdNode);
					return false;
				}
			} else {
				queue.take();
			}
		}
		// 已获得全局锁
		isLocked = true;
		logger.debug("Thread {} just acquired the global lock.", Thread.currentThread().getId());
		
		return true;
	}
	
	private final boolean ifNotFirstThenAddWatcher(ZooKeeper zk) throws Exception {
		String previousNode = this.getPreviousNodeNameInQueue(zk);
		if (previousNode == null) {
			return true;
		} else {
			// 在序列前一个结点注册Watcher，等待...
			Stat stat = zk.exists(buildPathForNode(previousNode), new NodeWatcher(zk));
			if (stat == null) {
				// 可能代码从 zk.getChildren 执行到当前行这个过程中，前面的结点被删除了，当前结点有可能变成最大了，所以重新检查一遍
				logger.debug("The node before me may be removed. Retry.");
				return ifNotFirstThenAddWatcher(zk);
			}
			return false;
		}
	}
	
	private String getPreviousNodeNameInQueue(ZooKeeper zk) throws Exception {
		List<String> childrenNames = zk.getChildren(this.node, false);
		if (childrenNames == null || childrenNames.size() == 0) {
			throw new LockException(String.format("No child found for node '%s'.", this.node));
		}
		
		if (childrenNames.size() == 1) {
			// 子节点只有自己
			NodeInfo ni = this.parseNodeInfo(childrenNames.get(0));
			this.mySeq = ni.getSeq();
			return null;
		}
		
		TreeMap<Long, String> treeMap = new TreeMap<Long, String>();
		mySeq = null;
		for (String name : childrenNames) {
			NodeInfo ni = this.parseNodeInfo(name);
			treeMap.put(ni.getSeq(), name);
			if (ni.getSessionId() == zk.getSessionId()) {
				this.mySeq = ni.getSeq();
			}
		}
		if (mySeq == null) {
			throw new LockException(
					String.format("Child created for node '%s' but can't be found. sessionId=%d", this.node, zk.getSessionId()));
		}
		Entry<Long, String> previous = treeMap.lowerEntry(mySeq);
		if (previous == null) {
			return null;
		} else {
			return previous.getValue();
		}
	}
	
	private void deleteNodeWithLogging(ZooKeeper zk, long seq) throws InterruptedException, KeeperException {
		String node = this.buildPathForNode(zk.getSessionId(), seq);
		zk.delete(node, -1);
		logger.debug("Zookeeper ephemeral node '{}' deleted.", node);
	}
	
	private void deleteNodeWithLogging(ZooKeeper zk, String node) throws InterruptedException, KeeperException {
		zk.delete(node, -1);
		logger.debug("Zookeeper ephemeral node '{}' deleted.", node);
	}

	@Override
	protected void unlockGlobal() throws Exception {
		if (mySeq == null) {
			throw new IllegalStateException("Can't get seq for the node to be removed.");
		}
		if (!isLocked) {
			throw new IllegalStateException("Already unlocked!");
		}
		ZooKeeper zk = this.zkProvider.get();
		deleteNodeWithLogging(zk, mySeq);
		isLocked = false;
	}
	
	private String buildPathForNewNode(long sessionId) {
		return String.format("%s/session-%d-", this.node, sessionId);
	}
	
	private String buildPathForNode(long sessionId, long seq) {
		return String.format("%s/session-%d-%010d", this.node, sessionId, seq);
	}
	
	private String buildPathForNode(String name) {
		return String.format("%s/%s", this.node, name);
	}
	
	private NodeInfo parseNodeInfo(String path) {
		// path format: "${parent}/session-${zk_sessionid}-${seq}"
		String[] ss = path.split("-");
		NodeInfo ni = new NodeInfo();
		ni.setSessionId(Long.parseLong(ss[ss.length - 2]));
		ni.setSeq(Long.parseLong(ss[ss.length - 1]));
		return ni;
	}
	
	private static class NodeInfo {
		private long sessionId;
		private long seq;

		public long getSessionId() {
			return sessionId;
		}

		public void setSessionId(long sessionId) {
			this.sessionId = sessionId;
		}

		public long getSeq() {
			return seq;
		}

		public void setSeq(long seq) {
			this.seq = seq;
		}
	}

	private class NodeWatcher implements Watcher {
		
		private final ZooKeeper zk;
		
		NodeWatcher(ZooKeeper zk) {
			this.zk = zk;
		}
		
		@Override
		public void process(WatchedEvent event) {
			logger.debug("Event fired on path: '{}', state: '{}', type: '{}'.",
					new Object[] {event.getPath(), event.getState().toString(), event.getType().toString()});
			if (event.getType() == org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted) {
				try {
					if (ifNotFirstThenAddWatcher(this.zk)) {
						// 已获得全局锁
						queue.put(new byte[0]);
					}
				} catch (Exception e) {
					logger.error(e.toString(), e);
				}
			}
		}

	}
	
}
