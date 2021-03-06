package org.radargun.ycsb;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.VacationStressor;
import org.radargun.stressors.AbstractCacheWrapperStressor;
import org.radargun.ycsb.transaction.InsertTx;
import org.radargun.ycsb.transaction.RMW;
import org.radargun.ycsb.transaction.Read;
import org.radargun.ycsb.transaction.YCSBTransaction;

public class YCSBStressor extends AbstractCacheWrapperStressor implements Runnable {

    private static Log log = LogFactory.getLog(VacationStressor.class);

    public static final int TEST_PHASE = 2;
    public static final int SHUTDOWN_PHASE = 3;
    
    volatile protected int m_phase = TEST_PHASE;
    
    private CacheWrapper cacheWrapper;
    private int threadid;
    private int multiplereadcount;
    private int recordCount;
    private int nodes;
    private int remote;
    private boolean totalOrder;
    
    private long restarts = 0;
    private long throughput = 0;

    public static final ThreadLocal<Integer> THREADID = new ThreadLocal<Integer>() {};
    public static int CLIENTS;
    public static int MY_NODE;
    
    public static Random r = new Random();

    public void setCacheWrapper(CacheWrapper cacheWrapper) {
	this.cacheWrapper = cacheWrapper;
    }
    
    @Override
    public void run() {
	stress(cacheWrapper);
    }
    
    private YCSBTransaction generateNextTransaction() {
        int ran = (Math.abs(r.nextInt())) % 100;
        int keynum = (Math.abs(r.nextInt())) % recordCount;
        if (ran < YCSB.readOnly) {
            return new Read(keynum);
        } else {
            return new InsertTx(keynum, Math.abs(r.nextInt()), remote);
            // return new RMW(keynum, Math.abs(r.nextInt()), remote, multiplereadcount, recordCount, totalOrder);
        }
    }
    
    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
	THREADID.set(this.threadid);
	
	this.cacheWrapper = wrapper;
	
	while (m_phase == TEST_PHASE) {
	    processTransaction(wrapper, generateNextTransaction());
	    this.throughput++;
	}

	Map<String, String> results = new LinkedHashMap<String, String>();

	return results;
    }
    
    private void processTransaction(CacheWrapper wrapper, YCSBTransaction transaction) {
	boolean successful = true;

	while (true) {
	    if (m_phase != TEST_PHASE) {
		this.throughput--;
		break;
	    }
	    cacheWrapper.startTransaction(transaction.isReadOnly());
	    try {
		transaction.executeTransaction(cacheWrapper);
	    } catch (Throwable e) {
		successful = false;
	    }

	    try {
		cacheWrapper.endTransaction(successful);

		if (!successful) {
		    setRestarts(getRestarts() + 1);
		}
	    } catch (Throwable rb) {
		setRestarts(getRestarts() + 1);
		successful = false;
	    }
	    
	    if (! successful) {
		successful = true;
	    } else { 
		break;
	    }
	}
    }

    @Override
    public void destroy() throws Exception {
	
    }

    public int getMultiplereadcount() {
        return multiplereadcount;
    }

    public void setMultiplereadcount(int multiplereadcount) {
        this.multiplereadcount = multiplereadcount;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(int recordcount) {
        this.recordCount = recordcount;
    }

    public int getNodes() {
        return nodes;
    }

    public void setNodes(int nodes) {
        this.nodes = nodes;
    }

    public int getRemote() {
        return remote;
    }

    public void setRemote(int remote) {
        this.remote = remote;
    }

    public long getRestarts() {
        return restarts;
    }

    public void setRestarts(long restarts) {
        this.restarts = restarts;
    }

    public long getThroughput() {
        return throughput;
    }

    public void setThroughput(long throughput) {
        this.throughput = throughput;
    }
    
    public void setThreadId(int threadid) {
	this.threadid = threadid;
    }
 
    public void setPhase(int shutdownPhase) {
	this.m_phase = shutdownPhase;
    }

    public void setTotalOrder(boolean totalOrder) {
	this.totalOrder = totalOrder;
    }

    
}
