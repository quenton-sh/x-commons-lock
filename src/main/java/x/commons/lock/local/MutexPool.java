package x.commons.lock.local;

import org.apache.commons.collections4.map.LRUMap;

public class MutexPool {

	private final LRUMap<String, byte[]> lruMap;
	
	public MutexPool(int size) {
		this.lruMap = new LRUMap<String, byte[]>(size);
	}
	
	public Object getMutex(String key) {
		byte[] mutex = lruMap.get(key);
		if (mutex == null) {
			synchronized (lruMap) {
				mutex = lruMap.get(key);
				if (mutex == null) {
					mutex = new byte[0];
					lruMap.put(key, mutex);
				}
			}
		}
		return mutex;
	}
	
	public static MutexPool defaultMutexPool() {
		return InstanceHolder.instance;
	}
	
	private static class InstanceHolder {
		static MutexPool instance = new MutexPool(10000);
	}
}
