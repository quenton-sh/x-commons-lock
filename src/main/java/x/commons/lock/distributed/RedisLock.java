package x.commons.lock.distributed;

import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.IOUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

public class RedisLock extends AbstractReentrantLock {
	
	private final JedisPool jedisPool;
	private final String password;
	private final String key;
	private final long autoReleaseTimeMillis;
	private final int retryMinDelayMillis;
	private final int retryMaxDelayMillis;
	private final Random random = new Random();
	private final String id = UUID.randomUUID().toString();
	
	/**
	 * 获取锁失败，重试的延时时间为5ms
	 * @param jedisPool
	 * @param password
	 * @param key
	 * @param autoReleaseTimeMillis
	 */
	public RedisLock(JedisPool jedisPool, String password, String key, long autoReleaseTimeMillis) {
		this(jedisPool, password, key, autoReleaseTimeMillis, 5, 5);
	}
	
	/**
	 * 
	 * @param jedisPool
	 * @param password
	 * @param key
	 * @param autoReleaseTimeMillis
	 * @param retryDelayMillis 获取锁失败，重试的延时时间下限(ms)
	 * @param retryMaxDelayMillis 获取锁失败，重试的延时时间上限(ms)
	 */
	public RedisLock(JedisPool jedisPool, String password, String key, long autoReleaseTimeMillis, int retryMinDelayMillis, int retryMaxDelayMillis) {
		if (retryMaxDelayMillis < retryMinDelayMillis) {
			throw new IllegalArgumentException("The value of 'retryMaxDelayMillis' must be greater than or equal to that of 'retryMinDelayMillis'.");
		}
		if (retryMinDelayMillis <= 0 || retryMaxDelayMillis <= 0) {
			throw new IllegalArgumentException("Neither 'retryMinDelayMillis' nor 'retryMaxDelayMillis' could be less than or equal to zero.");
		}
		this.jedisPool = jedisPool;
		this.password = password;
		this.key = key;
		this.autoReleaseTimeMillis = autoReleaseTimeMillis;
		this.retryMinDelayMillis = retryMinDelayMillis;
		this.retryMaxDelayMillis = retryMaxDelayMillis;
	}
	
	@Override
	protected boolean lockGlobal(long maxWaitTimeMillis) throws Exception {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			long waitTimeMillis  = maxWaitTimeMillis;
			boolean acquired = false;
			do {
				long startTs = System.currentTimeMillis();
				String ret = jedis.set(this.key, this.id, "NX", "PX", this.autoReleaseTimeMillis); // set if not exist
				acquired = "OK".equals(ret);
				if (acquired) {
					return true;
				}
				if (waitTimeMillis > 0) {
					waitTimeMillis -= System.currentTimeMillis() - startTs;
					if (waitTimeMillis <= 0) {
						// 超时
						return false;
					}
				}
				long retryDelayMillis = this.getRetryDelayMillis();
				Thread.sleep(retryDelayMillis);
			} while(true);
		} finally {
			IOUtils.closeQuietly(jedis);
		}
	}
	
	@Override
	protected void unlockGlobal() throws Exception {
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
		} finally {
			IOUtils.closeQuietly(jedis);
		}
	}

	private long getRetryDelayMillis() {
		if (this.retryMinDelayMillis == this.retryMaxDelayMillis) {
			return this.retryMinDelayMillis;
		} else {
			return this.retryMinDelayMillis + this.random.nextInt(this.retryMaxDelayMillis - this.retryMinDelayMillis);
		}
	}
	
	private Jedis getJedis() {
		Jedis jedis = this.jedisPool.getResource();
		if (this.password != null) {
			jedis.auth(this.password);
		}
		return jedis;
	}
}
