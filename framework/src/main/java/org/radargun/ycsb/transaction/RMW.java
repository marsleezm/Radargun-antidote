package org.radargun.ycsb.transaction;

import java.util.HashMap;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.ycsb.ByteIterator;
import org.radargun.ycsb.RandomByteIterator;
import org.radargun.ycsb.StringByteIterator;
import org.radargun.ycsb.YCSB;
import org.radargun.ycsb.YCSBStressor;

public class RMW extends YCSBTransaction {

    private int k;
    private int multiplereadcount;
    private int random;
    private int recordCount;
    private boolean totalOrder;
    
    public RMW(int k, int random, int remote, int multiplereadcount, int recordCount, boolean totalOrder) {
	super(Math.abs(random), remote);
	this.random = Math.abs(random);
	this.k = k;
	this.multiplereadcount = multiplereadcount;
	this.recordCount = recordCount;
	this.totalOrder = totalOrder;
    }

    @Override
    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
	HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

	for (int i=0; i< YCSB.fieldcount; i++) {
	    String fieldkey="field"+i;
	    ByteIterator data= new RandomByteIterator(YCSB.fieldlengthgenerator.nextInt());
	    values.put(fieldkey,data);
	}
	
	Map<String, String> row = StringByteIterator.getStringMap(values);
	int toWrite = (Math.abs(k * random)) % multiplereadcount;
	for (int i = 0 ; i < multiplereadcount; i++) {
	    LocatedKey key;
//	    if (super.remote && totalOrder && (i % 2 == 0)) {
//		int otherNode = (super.node + 1) % YCSBStressor.CLIENTS;
//		key = cacheWrapper.createKey("user" + (i % recordCount) + "-" + otherNode, otherNode);
//	    } else {
		key = cacheWrapper.createKey("user" + (i % recordCount) + "-" + super.node, super.node);
//	    }
	    
	    if (!super.remote && toWrite == i) {
		cacheWrapper.put(null, key, row);
	    } else {
		cacheWrapper.get(null, key);
	    }
	}
	
	if (remote) {
            LocatedKey key = cacheWrapper.createKey("local" + super.node + "-" + (((super.node + 1) * 1000000) + ((YCSBStressor.MY_NODE + 1) * 100) + (YCSBStressor.THREADID.get() + 1)), super.node);
	    cacheWrapper.put(null, key, row);
	}
	
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }
}
