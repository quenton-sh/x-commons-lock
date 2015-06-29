package x.commons.lock.distributed;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import x.commons.lock.LockException;

public class RedisLockTest extends RedisLockTestCommons {
	
	protected static int autoReleaseTimeMillis = 10000;
	protected static int retryMinDelayMillis = 5;
	protected static int retryMaxDelayMillis = 10;
	
	@BeforeClass
	public static void init() throws Exception {
		_init(autoReleaseTimeMillis, retryMinDelayMillis, retryMaxDelayMillis);
	}
	
	@AfterClass
	public static void cleanup() throws Exception {
		_cleanup();
	}
	
	@Test
	public void testConstruct() {
		@SuppressWarnings("unused")
		RedisLock sug = null;
		try {
			sug = new RedisLock(null, null, 5000, 3, 1);
			fail();
		} catch (IllegalArgumentException e) {
			// pass
			System.out.println(e.getMessage());
		}
		
		try {
			sug = new RedisLock(null, null, 5000, 0, 0);
			fail();
		} catch (IllegalArgumentException e) {
			// pass
			System.out.println(e.getMessage());
		}
		
		try {
			sug = new RedisLock(null, null, 5000, -1, -1);
			fail();
		} catch (IllegalArgumentException e) {
			// pass
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void test1() throws Exception {
		// one thread, no timeout
		assertTrue(!lock.isLocked());
		assertTrue(!lock.isHeldByCurrentThread());
		
		lock.lock();
		assertTrue(lock.isLocked());
		assertTrue(lock.isHeldByCurrentThread());
		assertTrue(lock.getHoldCount() == 1);
		
		lock.lock();
		assertTrue(lock.isLocked());
		assertTrue(lock.isHeldByCurrentThread());
		assertTrue(lock.getHoldCount() == 2);
		
		lock.unlock();
		assertTrue(lock.isLocked());
		assertTrue(lock.isHeldByCurrentThread());
		assertTrue(lock.getHoldCount() == 1);
		
		lock.unlock();
		assertTrue(!lock.isLocked());
		assertTrue(!lock.isHeldByCurrentThread());
		assertTrue(lock.getHoldCount() == 0);

		try {
			lock.unlock();
			fail();
		} catch (LockException e) {
		}
	}
	
	@Test
	public void test2() throws Exception {
		// one thread, with timeout
		boolean ret = lock.lock(1);
		assertTrue(!ret);
		assertTrue(!lock.isLocked());
	}
	
	@Test
	public void test3() throws Exception {
		// two threads, no timeout
		Thread t1 = new Thread(new TestRunnable("R1", 4000, lock, 0));
		Thread t2 = new Thread(new TestRunnable("R2", 10, lock, 0));
		
		t1.start();
		Thread.sleep(5);
		t2.start();
		
		t1.join();
		t2.join();
		System.out.println("main thread quit.");
	}
	
	@Test
	public void test4() throws Exception {
		// two threads, with timeout
		Thread t1 = new Thread(new TestRunnable("R1", 4000, lock, 0));
		Thread t2 = new Thread(new TestRunnable("R2", 10, lock, 1000));
		
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
		private RedisLock lock;
		private long waitTimeout;
		public TestRunnable(String name, long sleepTime, RedisLock lock, long waitTimeout) {
			this.name = name;
			this.sleepTime = sleepTime;
			this.lock = lock;
			this.waitTimeout = waitTimeout;
		}
		@Override
		public void run() {
			System.out.println(String.format("Job-%s start.", name));
			try {
				if (!this.lock.lock(waitTimeout)) {
					System.out.println(String.format("Job-%s timeout.", name));
					return;
				}
				
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			} catch (LockException e) {
				e.printStackTrace(System.err);
			} finally {
				if (lock.isLocked()) {
					try {
						lock.unlock();
					} catch (LockException e) {
						
					}
				}
			}
			
			System.out.println(String.format("Job-%s finished.", name));
		}
	}
}
