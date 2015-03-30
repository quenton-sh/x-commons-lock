package x.commons.lock;


public interface SimpleLock {

	public void lock() throws LockException;
	
	public boolean lock(long maxWaitTimeMillis) throws LockException;
	
	public void unlock() throws LockException;
	
	public boolean isLocked();
	
}
