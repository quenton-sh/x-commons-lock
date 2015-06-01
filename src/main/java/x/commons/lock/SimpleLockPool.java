package x.commons.lock;


public interface SimpleLockPool {

	public SimpleLock getLock(String key) throws LockException;
}
