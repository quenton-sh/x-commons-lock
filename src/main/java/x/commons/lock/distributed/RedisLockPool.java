package x.commons.lock.distributed;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;
import x.commons.lock.LockException;
import x.commons.lock.SimpleLock;

public class RedisLockPool extends AbstractLockPool {
	
	private final Pool<Jedis> jedisPool;
	private final int autoReleaseTimeMillis;
	private final int retryMinDelayMillis;
	private final int retryMaxDelayMillis;
	private final int failRetryCount; // 失败重试次数
	private final int failRetryIntervalMillis; // 失败多次重试之间的间隔时间（毫秒）

	public RedisLockPool(int size, Pool<Jedis> jedisPool,
			int autoReleaseTimeMillis, int retryMinDelayMillis,
			int retryMaxDelayMillis,
			int failRetryCount, int failRetryIntervalMillis) {
		super(size);
		this.jedisPool = jedisPool;
		this.autoReleaseTimeMillis = autoReleaseTimeMillis;
		this.retryMinDelayMillis = retryMinDelayMillis;
		this.retryMaxDelayMillis = retryMaxDelayMillis;
		this.failRetryCount = failRetryCount;
		this.failRetryIntervalMillis = failRetryIntervalMillis;
	}

	@Override
	protected SimpleLock newLock(String key) throws LockException {
		return new RedisLock(jedisPool, key, autoReleaseTimeMillis, 
				retryMinDelayMillis, retryMaxDelayMillis,
				failRetryCount, failRetryIntervalMillis);
	}

}
