package x.commons.lock.distributed;

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

public class ZookeeperLockTest {

	private static ZooKeeper zk;
	private static ZooKeeperLock lock;
	
	@BeforeClass
	public static void init() throws Exception {
		DOMConfigurator.configure(ZookeeperLockTest.class.getResource("/log4j.xml").getPath());
		
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
	
	@Test
	public void test1() throws Exception {
		assertTrue(!lock.isLocked());
		
		lock.lock();
		assertTrue(lock.isLocked());
		
		lock.unlock();
		assertTrue(!lock.isLocked());
	}
	
	@Test
	public void test2() throws Exception {
		Thread t1 = new Thread(new TestRunnable("R1", 4000, lock));
		Thread t2 = new Thread(new TestRunnable("R2", 10, lock));
		t1.setDaemon(true);
		t2.setDaemon(true);
		
		t1.start();
		Thread.sleep(5);
		t2.start();
		
		Thread.sleep(5000);
		System.out.println("main thread quit.");
	}
	
	public static void main(String[] args) throws Exception {
		String name = args[0];
		long sleepTime = Long.parseLong(args[1]);
		
		init();
		
		System.out.println(name + " start.");
		lock.lock();
		doSomething("method once", sleepTime);
		doSomething("method twice", sleepTime);
		lock.unlock();
		System.out.println(name + " finished.");
		
		cleanup();
	}
	
	public static void doSomething(String name, long sleepTime) throws Exception {
		System.out.println("Entering " + name + ".");
		lock.lock();
		Thread.sleep(sleepTime);
		System.out.println("Quiting " + name + ".");
	}
	
	private static class TestRunnable implements Runnable {
		private String name;
		private long sleepTime;
		private ZooKeeperLock lock;
		public TestRunnable(String name, long sleepTime, ZooKeeperLock lock) {
			this.name = name;
			this.sleepTime = sleepTime;
			this.lock = lock;
		}
		@Override
		public void run() {
			System.out.println(String.format("Job-%s start.", name));
			this.lock.lock();
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			this.lock.unlock();
			System.out.println(String.format("Job-%s finished.", name));
		}
	}
	
	@AfterClass
	public static void cleanup() throws Exception {
		zk.close();
		zk = null;
		
		lock = null;
	}
}
