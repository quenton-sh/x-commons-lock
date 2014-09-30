package x.commons.lock.util.distributed;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.xml.DOMConfigurator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import x.commons.lock.LockException;
import x.commons.lock.SimpleLock;
import x.commons.lock.distributed.ZooKeeperLockTestCommons;

public class ZooKeeperLockPoolTest {
	
	protected static ZooKeeper zk;
	protected static final String nodePath = "/tmp/locktest";
	
	@BeforeClass
	public static void init() throws Exception {
		DOMConfigurator.configure(ZooKeeperLockTestCommons.class.getResource("/log4j.xml").getPath());
		
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
	}
	
	@AfterClass
	public static void cleanup() throws Exception {
		zk.close();
		zk = null;
	}

	@Test
	public void getLock() throws LockException {
		ZooKeeperLockPool sug = new ZooKeeperLockPool(zk, nodePath, 1);
		
		SimpleLock lock1 = sug.getLock("key1");
		SimpleLock lock2 = sug.getLock("key2");
		assertTrue(lock1 != lock2);
		
		SimpleLock anotherLock1 = sug.getLock("key1");
		assertTrue(lock1 != anotherLock1);
		
		sug = new ZooKeeperLockPool(zk, nodePath, 5);
		lock1 = sug.getLock("key1");
		lock2 = sug.getLock("key2");
		assertTrue(lock1 != lock2);
		
		anotherLock1 = sug.getLock("key1");
		assertTrue(lock1 == anotherLock1);
	}
	
	/*
	 * 不同key测试
	 */
	@Test
	public void test1() throws Exception {
		ZooKeeperLockPool sug = new ZooKeeperLockPool(zk, nodePath, 5);
		
		// R1应该不阻塞R2
		Thread t1 = new Thread(new TestRunnable("R1", "key1", 4000, sug));
		Thread t2 = new Thread(new TestRunnable("R2", "key2", 10, sug));
		
		t1.start();
		Thread.sleep(5);
		t2.start();

		t1.join();
		t2.join();
		System.out.println("main thread quit.");
	}
	
	/**
	 * 相同key测试
	 * @throws Exception
	 */
	@Test
	public void test2() throws Exception {
		ZooKeeperLockPool sug = new ZooKeeperLockPool(zk, nodePath, 5);
		
		// R1应该阻塞住R2
		Thread t1 = new Thread(new TestRunnable("R3", "key1", 4000, sug));
		Thread t2 = new Thread(new TestRunnable("R4", "key1", 10, sug));
		
		t1.start();
		Thread.sleep(5);
		t2.start();

		t1.join();
		t2.join();
		System.out.println("main thread quit.");
	}
	
	private static class TestRunnable implements Runnable {
		private String name;
		private String key;
		private long sleepTime;
		private ZooKeeperLockPool lockPool;
		public TestRunnable(String name, String key, long sleepTime, ZooKeeperLockPool lockPool) {
			this.name = name;
			this.key = key;
			this.sleepTime = sleepTime;
			this.lockPool = lockPool;
		}
		@Override
		public void run() {
			System.out.println(String.format("Job-%s start.", name));
			try {
				this.lockPool.getLock(key).lock();
				
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				this.lockPool.getLock(key).unlock();
			} catch (LockException e) {
				e.printStackTrace(System.err);
			}
			
			System.out.println(String.format("Job-%s finished.", name));
		}
	}
}
