package org.radargun.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.domain.IntSet;

public class ContainsTransaction implements MicrobenchmarkTransaction {

    public final int node;
    public final int value;

    public ContainsTransaction(int node, int value) {
        this.node = node;
        this.value = value;
    }

    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
        LocatedKey key = cacheWrapper.createKey("SET" + node, node);
	IntSet intset = ((IntSet)cacheWrapper.get(null, key));
        return intset.contains(cacheWrapper, this.value);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }
}
