package x.commons.lock.distributed;

import redis.clients.jedis.JedisPool;
import x.commons.lock.LockException;
import x.commons.lock.SimpleLock;

public class RedisLockPool extends AbstractLockPool {
	
	private final JedisPool jedisPool;
	private final int autoReleaseTimeMillis;
	private final int retryMinDelayMillis;
	private final int retryMaxDelayMillis;

	public RedisLockPool(int size, JedisPool jedisPool,
			int autoReleaseTimeMillis, int retryMinDelayMillis,
			int retryMaxDelayMillis) {
		super(size);
		this.jedisPool = jedisPool;
		this.autoReleaseTimeMillis = autoReleaseTimeMillis;
		this.retryMinDelayMillis = retryMinDelayMillis;
		this.retryMaxDelayMillis = retryMaxDelayMillis;
	}

	@Override
	protected SimpleLock newLock(String key) throws LockException {
		return new RedisLock(jedisPool, key, autoReleaseTimeMillis, retryMinDelayMillis, retryMaxDelayMillis);
	}

}
