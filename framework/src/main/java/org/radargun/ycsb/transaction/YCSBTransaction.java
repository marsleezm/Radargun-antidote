package org.radargun.ycsb.transaction;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.VacationStressor;
import org.radargun.ycsb.YCSBStressor;

public abstract class YCSBTransaction {

    protected boolean remote;
    protected int node;
    
    public YCSBTransaction(int random, int remotePerc) {
	this.remote = (random % 100) < remotePerc;
	if (this.remote) {
	    this.node = random % YCSBStressor.CLIENTS;
	} else {
	    this.node = YCSBStressor.MY_NODE;
	}
    }
    
    public abstract void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;
    
    public abstract boolean isReadOnly();
}
    
