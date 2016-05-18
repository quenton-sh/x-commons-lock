package x.commons.lock.distributed;

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;
import x.commons.util.Provider;

public class JedisPoolProvider implements Provider<Pool<Jedis>> {
	
	private final Pool<Jedis> jedisPool;
	
	public JedisPoolProvider(Pool<Jedis> jedisPool) {
		this.jedisPool = jedisPool;
	}

	@Override
	public Pool<Jedis> get() {
		return this.jedisPool;
	}

	@Override
	public Pool<Jedis> get(Object... arg0) {
		return this.jedisPool;
	}

	@Override
	public Pool<Jedis> get(Map<String, Object> arg0) {
		return this.jedisPool;
	}

}
