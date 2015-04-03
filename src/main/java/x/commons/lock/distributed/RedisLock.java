package x.commons.lock.distributed;

import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.IOUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisLock extends AbstractReentrantLock {
	
	private final JedisPool jedisPool;
	private final String key;
	private final long autoReleaseTimeMillis;
	private final int retryMinDelayMillis;
	private final int retryMaxDelayMillis;
	private final Random random = new Random();
	private final String id = UUID.randomUUID().toString();
	
	public RedisLock(JedisPool jedisPool, String key, long autoReleaseTimeMillis) {
		this(jedisPool, key, autoReleaseTimeMillis, 1, 1);
	}
	
	/**
	 * 
	 * @param jedisPool
	 * @param key
	 * @param autoReleaseTimeMillis
	 * @param retryDelayMillis 获取锁失败，重试的延时时间下限
	 * @param retryMaxDelayMillis 获取锁失败，重试的延时时间上限
	 */
	public RedisLock(JedisPool jedisPool, String key, long autoReleaseTimeMillis, int retryMinDelayMillis, int retryMaxDelayMillis) {
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
	}
	
	@Override
	protected boolean lockGlobal(long maxWaitTimeMillis) throws Exception {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
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
			jedis = jedisPool.getResource();
			// 删除锁结点
			String val = jedis.get(this.key);
			if (val != null && val.equals(this.id)) {
				// 这里有并发问题：get后如果key expire掉，并且被其他进程set获得锁，则此命令将删除其他进程持有的锁
				// Redisson源码中也有这个问题，不使用Transaction情况下无解
				jedis.del(this.key);
			}
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
}
