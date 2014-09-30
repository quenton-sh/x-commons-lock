package x.commons.lock.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class MutexPoolTest {

	@Test
	public void getLock() {
		MutexPool sug = new MutexPool(1);
		
		Object mutex1 = sug.getMutex("key1");
		Object mutex2 = sug.getMutex("key2");
		assertTrue(mutex1 != mutex2);
		
		Object anotherMutex1 = sug.getMutex("key1");
		assertTrue(mutex1 != anotherMutex1);
		
		sug = new MutexPool(5);
		mutex1 = sug.getMutex("key1");
		mutex2 = sug.getMutex("key2");
		assertTrue(mutex1 != mutex2);
		
		anotherMutex1 = sug.getMutex("key1");
		assertTrue(mutex1 == anotherMutex1);
	}
}
