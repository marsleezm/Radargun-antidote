package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.ycsb.YCSB;
import org.radargun.ycsb.YCSBPopulationStressor;

public class YCSBPopulationStage extends AbstractDistStage {
    
    private int recordCount;
    private boolean totalOrder;
    
    public void setRecordCount(int recordCount) {
	this.recordCount = recordCount;
    }
    
    public void setTotalOrder(boolean totalOrder) {
	this.totalOrder = totalOrder;
    }
    
    @Override
    public DistStageAck executeOnSlave() {
	YCSB.preinit();
	DefaultDistStageAck ack = newDefaultStageAck();
	CacheWrapper wrapper = slaveState.getCacheWrapper();
	if (wrapper == null) {
	    log.info("Not executing any test as the wrapper is not set up on this slave ");
	    return ack;
	}
	if (this.totalOrder) {
	    wrapper.setupTotalOrder();
	}
	long startTime = System.currentTimeMillis();
	populate(wrapper);
	long duration = System.currentTimeMillis() - startTime;
	log.info("The population took: " + (duration / 1000) + " seconds.");
	ack.setPayload(duration);
	return ack;
    }

    private void populate(CacheWrapper wrapper) {
	YCSBPopulationStressor ycsbStressor = new YCSBPopulationStressor();
	ycsbStressor.setRecordCount(recordCount);
	ycsbStressor.stress(wrapper);
    }
}
