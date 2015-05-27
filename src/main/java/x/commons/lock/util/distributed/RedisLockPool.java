package x.commons.lock.util.distributed;

import redis.clients.jedis.JedisPool;
import x.commons.lock.LockException;
import x.commons.lock.SimpleLock;
import x.commons.lock.distributed.RedisLock;

public class RedisLockPool extends AbstractLockPool {
	
	private final JedisPool jedisPool;
	private final String password;
	private final long autoReleaseTimeMillis;

	public RedisLockPool(JedisPool jedisPool, String password, long autoReleaseTimeMillis, int size) {
		super(size);
		this.jedisPool = jedisPool;
		this.password = password;
		this.autoReleaseTimeMillis = autoReleaseTimeMillis;
	}

	@Override
	protected SimpleLock newLock(String key) throws LockException {
		return new RedisLock(jedisPool, password, key, autoReleaseTimeMillis);
	}

}
