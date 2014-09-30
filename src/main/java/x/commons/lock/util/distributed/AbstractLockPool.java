package x.commons.lock.util.distributed;

import org.apache.commons.collections4.map.LRUMap;

import x.commons.lock.LockException;
import x.commons.lock.SimpleLock;

abstract class AbstractLockPool {
	
	private final LRUMap<String, SimpleLock> lruMap;
	
	AbstractLockPool(int size) {
		this.lruMap = new LRUMap<String, SimpleLock>(size);
	}

	public SimpleLock getLock(String key) throws LockException {
		SimpleLock lock = lruMap.get(key);
		if (lock == null) {
			synchronized (lruMap) {
				lock = lruMap.get(key);
				if (lock == null) {
					lock = this.newLock(key);
					if (lock != null) {
						lruMap.put(key, lock);
					}
				}
			}
		}
		return lock;
	}
	
	
	protected abstract SimpleLock newLock(String key) throws LockException;
	
}
