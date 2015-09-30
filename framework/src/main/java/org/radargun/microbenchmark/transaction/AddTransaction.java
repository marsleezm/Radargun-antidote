package org.radargun.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.MicrobenchmarkStressor;
import org.radargun.microbenchmark.domain.IntSet;

public class AddTransaction implements MicrobenchmarkTransaction {

    public final int node;
    public final int value;
    public final boolean local;
    public final boolean remote;
    public final boolean totalOrder;

    public AddTransaction(int node, int value, boolean local, boolean remote, boolean totalOrder) {
        this.node = node;
        this.value = value;
        this.local = local;
	this.remote = remote;
	this.totalOrder = totalOrder;
    }

    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
        LocatedKey key = cacheWrapper.createKey("SET" + node, node);
        IntSet set = (IntSet)cacheWrapper.get(null, key);
        boolean res = set.add(cacheWrapper, this.value, local, remote);
//        if (totalOrder && remote) {
//            int otherNode = (node + 1) % MicrobenchmarkStressor.clients;
//            key = cacheWrapper.createKey("SET" + otherNode, otherNode);
//            set = (IntSet)cacheWrapper.get(null, key);
//            set.add(cacheWrapper, this.value, local, remote);    
//        }
        return res;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

}
