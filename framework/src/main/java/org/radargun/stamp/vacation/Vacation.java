package org.radargun.stamp.vacation;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;

public class Vacation {

    public static final ThreadLocal<Integer> NODE_TARGET = new ThreadLocal<Integer>() {};
    
    public static final void put(CacheWrapper cacheWrapper, LocatedKey key, Object value) {
	try {
	    cacheWrapper.put(null, key, value);
	} catch (Exception e) {
	    if (e instanceof RuntimeException) {
		throw (RuntimeException)e;
	    }
	    e.printStackTrace();
	}
    }
    
    public static final <T> T get(CacheWrapper cacheWrapper, LocatedKey key) {
	try {
	    return (T) cacheWrapper.get(null, key);
	} catch (Exception e) {
	    if (e instanceof RuntimeException) {
		throw (RuntimeException)e;
	    }
	    e.printStackTrace();
	    return null;
	}
    }
    
}
