package x.commons.lock.distributed;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.commons.lock.LockException;
import x.commons.lock.SimpleLock;

public abstract class AbstractReentrantLock implements SimpleLock {
	
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final ReentrantLock reentrantLock = new ReentrantLock(true);
	private final ThreadLocal<byte[]> globalLockStatus = new ThreadLocal<byte[]>();
	
	@Override
	public void lock() throws LockException {
		this.lock(0);
	}
	
	@Override
	public boolean lock(long maxWaitTimeMillis) throws LockException {
		try {
			long waitTimeMillis = maxWaitTimeMillis;
			
			// 当前线程 在进程内 与其他线程竞争本地锁
			logger.debug("Thread {} try to acquire the local lock...", Thread.currentThread().getId());
			if (waitTimeMillis <= 0) {
				reentrantLock.lock();
			} else {
				long startTs = System.currentTimeMillis();
				if (!reentrantLock.tryLock(waitTimeMillis, TimeUnit.MILLISECONDS)) {
					logger.debug("Thread {} failed to acquire the local lock -- time out.", Thread.currentThread().getId());
					return false;
				}
				waitTimeMillis -= System.currentTimeMillis() - startTs;
				if (waitTimeMillis <= 0) {
					logger.debug("Thread {} acquired the local lock, but no time left to proceed.", Thread.currentThread().getId());
					this.unlockLocalWithLogging();
					return false;
				}
			}
			logger.debug("Thread {} acquired the local lock, hold count={}.", Thread.currentThread().getId(), reentrantLock.getHoldCount());
			
			// 当前线程 代表本进程 与其他进程竞争全局锁
			if (globalLockStatus.get() == null) {
				// 未持有全局锁，尝试获取
				logger.debug("Thread {} try to acquire the global lock...", Thread.currentThread().getId());
				if (!this.lockGlobal(waitTimeMillis)) {
					// 获取全局锁超时，释放本地锁并退出
					logger.debug("Thread {} failed to acquire the global lock -- time out.", Thread.currentThread().getId());
					unlockLocalWithLogging();
					return false;
				}
				logger.debug("Thread {} acquired the global lock.", Thread.currentThread().getId());
				
				globalLockStatus.set(new byte[0]);
				logger.debug("Thread {} cached its global lock.", Thread.currentThread().getId());
				
			} else {
				// 已持有全局锁
				logger.debug("Thread {} already holds the global lock.", Thread.currentThread().getId());
			}
			
			return true;
		} catch (Exception e) {
			if (e instanceof LockException) {
				throw (LockException) e;
			} else {
				throw new LockException(e);
			}
		}
		/*
		// 异常发生后的解锁代码不要在此实现，
		// 应由调用者对lock()方法进行异常捕获，并在finally中调用unlock()来实现
		finally {
			reentrantLock.unlock(); 
		}
		*/
	}
	
	@Override
	public void unlock() throws LockException {
		if (!reentrantLock.isHeldByCurrentThread()) {
			// 当前线程未持有本地锁（根据加锁代码，此时也不会持有全局锁）：违反原则1，禁止解锁
			throw new LockException(String.format("Current thread %d doesn't hold the local lock.", Thread.currentThread().getId()));
		}
		try {
			// 本地锁被多次重入的情况下，仅对本地锁减少重入次数，不对全局锁做改动
			if (reentrantLock.getHoldCount() <= 1) {
				if (globalLockStatus.get() != null) {
					logger.debug("Thread {} try to release the global lock...", Thread.currentThread().getId());
					this.unlockGlobal();
					logger.debug("Thread {} released the global lock.", Thread.currentThread().getId());
					
					globalLockStatus.remove();
					logger.debug("Thread {} doesn't cache its global lock any more.", Thread.currentThread().getId());
				}
			}
		} catch (Exception e) {
			if (e instanceof LockException) {
				throw (LockException) e;
			} else {
				throw new LockException(e);
			}
		} finally {
			/*
			执行至此可能有3种情况：
			1. 本地锁多次重入，不解全局锁，仅减少本地锁重入次数
			2. 本地锁无多次重入（剩最后一层），解全局锁成功：解本地锁最后一层
			3. 本地锁无多次重入（剩最后一层），解全局锁失败（抛出异常）：
			   此时应将本地锁解掉，全局锁由超时自动释放机制来解；
			   否则一旦向上层应用抛出异常，上层应用可能会退出，没机会再次调用unlock来解本地锁，导致本地锁始终被当前线程持有，无法释放；
			   当同一个lock key由其他线程加锁时，将永远得不到锁
			*/
			
			this.unlockLocalWithLogging();
		}
	}
	
	private void unlockLocalWithLogging() {
		reentrantLock.unlock();
		logger.debug("Thread {} released the local lock, hold count={}.", Thread.currentThread().getId(), reentrantLock.getHoldCount());
	}
	
	@Override
	public boolean isLocked() {
		return reentrantLock.isLocked();
	}
	
	@Override
	public int getHoldCount() {
		return reentrantLock.getHoldCount();
	}
	
	@Override
	public boolean isHeldByCurrentThread() {
		return reentrantLock.isHeldByCurrentThread();
	}
	
	/**
	 * 获取全局锁
	 * @param waitTimeMillis
	 * @return true:获取成功; false:超时
	 * @throws Exception
	 */
	protected abstract boolean lockGlobal(long waitTimeMillis) throws Exception;
	
	protected abstract void unlockGlobal() throws Exception;
	
	@Override
	public void finalize() throws Throwable {
		super.finalize();
		if (this.reentrantLock.isHeldByCurrentThread() && this.reentrantLock.isLocked()) {
			this.reentrantLock.unlock();
		}
	}
	
}
