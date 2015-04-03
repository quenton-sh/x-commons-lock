package x.commons.lock.util.distributed;

import redis.clients.jedis.JedisPool;
import x.commons.lock.LockException;
import x.commons.lock.SimpleLock;
import x.commons.lock.distributed.RedisLock;

public class RedisLockPool extends AbstractLockPool {
	
	private JedisPool jedisPool;
	private long autoReleaseTimeMillis;

	public RedisLockPool(JedisPool jedisPool, long autoReleaseTimeMillis, int size) {
		super(size);
		this.jedisPool = jedisPool;
		this.autoReleaseTimeMillis = autoReleaseTimeMillis;
	}

	@Override
	protected SimpleLock newLock(String key) throws LockException {
		return new RedisLock(jedisPool, key, autoReleaseTimeMillis);
	}

}
