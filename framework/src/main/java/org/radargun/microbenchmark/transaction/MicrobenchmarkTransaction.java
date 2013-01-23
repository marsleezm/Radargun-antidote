package org.radargun.microbenchmark.transaction;

import org.radargun.CacheWrapper;

public interface MicrobenchmarkTransaction {

    boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable;

    boolean isReadOnly();
    
}
