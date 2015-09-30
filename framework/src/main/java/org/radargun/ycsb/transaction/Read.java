package org.radargun.ycsb.transaction;

import java.util.HashMap;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.ycsb.ByteIterator;
import org.radargun.ycsb.RandomByteIterator;
import org.radargun.ycsb.StringByteIterator;
import org.radargun.ycsb.YCSB;

public class Read extends YCSBTransaction {

    private int k;
    
    public Read(int k) {
	super(1, 0);
	this.k = k;
    }

    @Override
    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
	HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

	for (int i=0; i< YCSB.fieldcount; i++)
	{
	    String fieldkey="field"+i;
	    ByteIterator data= new RandomByteIterator(YCSB.fieldlengthgenerator.nextInt());
	    values.put(fieldkey,data);
	}
	LocatedKey key = cacheWrapper.createKey("user" + k + "-" + super.node, super.node);
	Map<String, String> row = (Map) cacheWrapper.get(null, key);
	HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
	if (row != null) {
	    result.clear();
	    StringByteIterator.putAllAsByteIterators(result, row);
	}
    }

    @Override
    public boolean isReadOnly() {
	return true;
    }
}
