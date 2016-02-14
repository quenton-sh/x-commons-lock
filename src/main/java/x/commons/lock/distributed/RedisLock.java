package x.commons.lock.distributed;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.util.Pool;
import x.commons.util.failover.RetrySupport;

/**
 * 使用随机延时重试策略的Redis分布式锁
 * <p>非公平锁</p>
 * <p>锁被持有时长达到超时时间后会自动释放</p>
 * <p>尝试获取锁时等待超时则获取失败</p>
 * 
 * @NotThreadSafe
 * @author Quenton
 *
 */
public class RedisLock extends AbstractReentrantLock {
	
	private final Pool<Jedis> jedisPool;
	private final String key;
	private final int autoReleaseTimeMillis;
	private final int retryMinDelayMillis;
	private final int retryMaxDelayMillis;
	private final int failRetryCount; // 失败重试次数
	private final int failRetryIntervalMillis; // 失败多次重试之间的间隔时间（毫秒）
	
	private final Random random = new Random();
	private final String id = UUID.randomUUID().toString();
	
	private boolean isLocked = false;
	
	/**
	 * 
	 * @param jedisPool
	 * @param key
	 * @param autoReleaseTimeMillis
	 * @param retryDelayMillis 获取锁失败，重试的延时时间下限(ms)
	 * @param retryMaxDelayMillis 获取锁失败，重试的延时时间上限(ms)
	 */
	public RedisLock(Pool<Jedis> jedisPool, String key, 
			int autoReleaseTimeMillis, int retryMinDelayMillis, int retryMaxDelayMillis,
			int failRetryCount, int failRetryIntervalMillis) {
		if (retryMaxDelayMillis < retryMinDelayMillis) {
			throw new IllegalArgumentException("The value of 'retryMaxDelayMillis' must be greater than or equal to that of 'retryMinDelayMillis'.");
		}
		if (retryMinDelayMillis <= 0 || retryMaxDelayMillis <= 0) {
			throw new IllegalArgumentException("Neither 'retryMinDelayMillis' nor 'retryMaxDelayMillis' could be less than or equal to zero.");
		}
		this.jedisPool = jedisPool;
		this.key = key;
		this.autoReleaseTimeMillis = autoReleaseTimeMillis;
		this.retryMinDelayMillis = retryMinDelayMillis;
		this.retryMaxDelayMillis = retryMaxDelayMillis;
		this.failRetryCount = failRetryCount;
		this.failRetryIntervalMillis = failRetryIntervalMillis;
	}
	
	@Override
	protected boolean lockGlobal(final long maxWaitTimeMillis) throws Exception {
		if (isLocked) {
			throw new IllegalStateException("Already locked!");
		}
		return new RetrySupport(this.failRetryCount, this.failRetryIntervalMillis)
				.callWithRetry(new Callable<Boolean>() {
					@Override
					public Boolean call() throws Exception {
						return doLockGlobal(maxWaitTimeMillis);
					}
				});
	}
	
	private boolean doLockGlobal(long maxWaitTimeMillis) throws Exception {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			
//			if (maxWaitTimeMillis > 0) {
//				logger.debug(String.format("Thread %d is trying to acquire the global lock: wait time left %d milliseconds.", Thread.currentThread().getId(), maxWaitTimeMillis));
//			}
			long startTs = System.currentTimeMillis();
			
			boolean acquired = false;
			do {
				long ellapsed = System.currentTimeMillis() - startTs;
				long waitTimeLeft = maxWaitTimeMillis - ellapsed;
//				long waitTimeLeftPrint = waitTimeLeft < 0 ? 0 : waitTimeLeft;
				if (maxWaitTimeMillis > 0) {
//					logger.debug(String.format("Thread %d is trying to acquire the global lock: wait time left %d milliseconds.", Thread.currentThread().getId(), waitTimeLeftPrint));
					if (waitTimeLeft <= 0) {
						// 超时
						return false;
					}
				}
				
				String ret = jedis.set(this.key, this.id, "NX", "PX", this.autoReleaseTimeMillis); // set if not exist
				acquired = "OK".equals(ret);
				if (acquired) {
					// 已获得全局锁
					isLocked = true;
					logger.debug(String.format("Thread %d just acquired the global lock.", Thread.currentThread().getId()));
					return true;
				}
				
				long retryDelayMillis = this.getRetryDelayMillis();
				logger.debug(String.format("Thread %d is trying to acquire the global lock: retry in %d milliseconds.", Thread.currentThread().getId(), retryDelayMillis));
				Thread.sleep(retryDelayMillis);
			} while(true);
		} finally {
			IOUtils.closeQuietly(jedis);
		}
	}
	
	@Override
	protected void unlockGlobal() throws Exception {
		if (!isLocked) {
			throw new IllegalStateException("Already unlocked!");
		}
		new RetrySupport(this.failRetryCount, this.failRetryIntervalMillis)
				.callWithRetry(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						doUnlockGlobal();
						return null;
					}
				});
	}
	
	private void doUnlockGlobal() throws Exception {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			// 删除锁结点，加乐观锁
			jedis.watch(this.key);
			String val = jedis.get(this.key);
			Transaction t = jedis.multi();
			if (val != null && val.equals(this.id)) {
				t.del(this.key);
			}
			t.exec(); // 无需检查结果，事务失败表示锁已过期，或已被其他进程持有
			isLocked = false;
		} finally {
			IOUtils.closeQuietly(jedis);
		}
	}

	private long getRetryDelayMillis() {
		if (this.retryMinDelayMillis == this.retryMaxDelayMillis) {
			return this.retryMinDelayMillis;
		} else {
			return this.retryMinDelayMillis + this.random.nextInt(this.retryMaxDelayMillis - this.retryMinDelayMillis + 1);
		}
	}
	
	private Jedis getJedis() {
		return this.jedisPool.getResource();
	}
}
