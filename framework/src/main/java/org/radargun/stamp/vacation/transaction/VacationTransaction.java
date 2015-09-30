package org.radargun.stamp.vacation.transaction;

import java.util.Random;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.VacationStressor;

public abstract class VacationTransaction {

    protected static CacheWrapper WRAPPER;
    
    protected boolean remote;
    protected int node;

    static Random r = new Random();
    
    public VacationTransaction(int random, int remotePerc) {
	this.remote = (random % 100) < remotePerc;
	if (this.remote) {
	    this.node = Math.abs(Math.abs(r.nextInt()) % VacationStressor.CLIENTS);
	} else {
	    this.node = VacationStressor.MY_NODE;
	}
    }
    
    public abstract void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;
    
    public abstract boolean isReadOnly();
}
