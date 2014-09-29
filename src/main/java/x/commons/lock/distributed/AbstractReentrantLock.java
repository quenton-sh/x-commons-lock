package x.commons.lock.distributed;

import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.commons.lock.LockException;
import x.commons.lock.SimpleLock;

public abstract class AbstractReentrantLock implements SimpleLock {
	
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final ReentrantLock reentrantLock = new ReentrantLock();
	private final ThreadLocal<byte[]> globalLockStatus = new ThreadLocal<byte[]>();
	
	private boolean isLocked = false;
	
	@Override
	public void lock() throws LockException {
		try {
			// 当前线程 在进程内 与其他线程竞争本地锁
			reentrantLock.lock();
			// 当前线程 代表本进程 与其他进程竞争全局锁
			if (globalLockStatus.get() == null) {
				// 未持有全局锁，尝试获取
				this.lockGlobal();
				globalLockStatus.set(new byte[0]);
				logger.debug(String.format("Thread %d cached its global lock.", Thread.currentThread().getId()));
			} else {
				// 已持有全局锁
				logger.debug(String.format("Thread %d already own a global lock.", Thread.currentThread().getId()));
			}
			
			isLocked = true;
		} catch (Exception e) {
			if (e instanceof LockException) {
				throw (LockException) e;
			} else {
				throw new LockException(e);
			}
		}
	}

	@Override
	public void unlock() throws LockException {
		if (!this.isLocked) {
			return;
		}
		if (!reentrantLock.isHeldByCurrentThread()) {
			throw new LockException("Current thread doesn't hold the local lock.");
		} else if (globalLockStatus.get() == null) {
			try {
				reentrantLock.unlock();
			} catch (IllegalMonitorStateException e) {
				// just ignore
			}
			throw new LockException("Current thread doesn't hold the global lock.");
		} else {
			try {
				this.unlockGlobal();
				globalLockStatus.remove();
				logger.debug(String.format("Thread %d didn't cache its global lock any more.", Thread.currentThread().getId()));
				isLocked = false;
				reentrantLock.unlock();
			} catch (Exception e) {
				if (e instanceof LockException) {
					throw (LockException) e;
				} else {
					throw new LockException(e);
				}
			}
		}
	}
	
	@Override
	public boolean isLocked() {
		return this.isLocked;
	}
	
	protected abstract void lockGlobal() throws Exception;
	
	protected abstract void unlockGlobal() throws Exception;
	
	
}
