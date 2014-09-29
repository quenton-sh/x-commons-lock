package x.commons.lock.distributed;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.xml.DOMConfigurator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ZooKeeperLockTestCommons {

	protected static ZooKeeper zk;
	protected static ZooKeeperLock lock;
	
	@BeforeClass
	public static void init() throws Exception {
		DOMConfigurator.configure(ZooKeeperLockTest.class.getResource("/log4j.xml").getPath());
		
		String hosts = "localhost:2181";
		int sessionTimeout = 2000;
		final CountDownLatch connectedSignal = new CountDownLatch(1); 
		zk = new ZooKeeper(hosts, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected) {  
		            connectedSignal.countDown();  
		        }
			}
		});
		connectedSignal.await(5, TimeUnit.SECONDS);
		
		lock = new ZooKeeperLock(zk, "/tmp/locktest");
	}
	
	@AfterClass
	public static void cleanup() throws Exception {
		zk.close();
		zk = null;
		
		lock = null;
	}
}
