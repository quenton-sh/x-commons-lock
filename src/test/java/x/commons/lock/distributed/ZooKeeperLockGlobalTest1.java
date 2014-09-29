package x.commons.lock.distributed;

/**
 * 测试jvm进程间的同步效果
 * @author Quenton
 *
 */
public class ZooKeeperLockGlobalTest1 extends ZooKeeperLockTestCommons {

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
}
