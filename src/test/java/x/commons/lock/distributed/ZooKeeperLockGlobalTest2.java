package x.commons.lock.distributed;

/**
 * 测试jvm进程间同步-设置锁超时的效果
 * @author Quenton
 *
 */
public class ZooKeeperLockGlobalTest2 extends ZooKeeperLockTestCommons {
	
	/*
	 
	private static final long lockTimeout = 5000;

	
	public static void main(String[] args) throws Exception {
		String name = args[0];
		long sleepTime = Long.parseLong(args[1]);
		
		init();
		ZooKeeperLock zlock = (ZooKeeperLock) lock;
		zlock.setLockTimeout(lockTimeout); // 设置锁超时时间
		
		System.out.println(name + " start.");
		zlock.lock();
		doSomething("method once", sleepTime);
		zlock.unlock();
		System.out.println(name + " finished.");
		
		cleanup();
	}
	
	public static void doSomething(String name, long sleepTime) throws Exception {
		System.out.println("Entering " + name + ".");
		Thread.sleep(sleepTime);
		System.out.println("Quiting " + name + ".");
	}
	
	 */
}
