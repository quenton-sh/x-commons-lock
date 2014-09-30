package x.commons.lock.distributed;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import x.commons.lock.LockException;

public class ZookeeperLockTest extends ZooKeeperLockTestCommons {

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
		
		t1.start();
		Thread.sleep(5);
		t2.start();
		
		t1.join();
		t2.join();
		System.out.println("main thread quit.");
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
			try {
				this.lock.lock();
				
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				this.lock.unlock();
			} catch (LockException e) {
				e.printStackTrace(System.err);
			}
			
			System.out.println(String.format("Job-%s finished.", name));
		}
	}
}
