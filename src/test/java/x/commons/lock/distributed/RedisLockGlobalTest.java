package x.commons.lock.distributed;


/**
 * 测试jvm进程间同步的效果
 * @author Quenton
 *
 */
public class RedisLockGlobalTest extends RedisLockTestCommons {
	
	public static void main(String[] args) throws Exception {
		// init
		String name = args[0];
		int autoReleaseTimeMillis = Integer.parseInt(args[1]);
		int retryMinDelayMillis = Integer.parseInt(args[2]);
		int retryMaxDelayMillis = Integer.parseInt(args[3]);
		int failRetryCount = 1;
		int failRetryIntervalMillis = 1;
		long sleepTime = Long.parseLong(args[4]);
		long waitTimeout = Long.parseLong(args[5]);
		_init(autoReleaseTimeMillis, retryMinDelayMillis, retryMaxDelayMillis,
				failRetryCount, failRetryIntervalMillis);
		
		// do test
		System.out.println(name + " start.");
		if (!lock.lock(waitTimeout)) {
			System.out.println(name + " timeout, quit.");
			return;
		}
		doSomething("method once", sleepTime);
		doSomething("method twice", sleepTime);
		lock.unlock();
		System.out.println(name + " finished.");
		
		// cleanup
		_cleanup();
	}
	
	
	
	private static void doSomething(String name, long sleepTime) throws Exception {
		System.out.println("Entering " + name + ".");
		lock.lock();
		Thread.sleep(sleepTime);
		System.out.println("Quiting " + name + ".");
		lock.unlock();
	}
}
