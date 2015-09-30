package org.radargun.stages;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.stamp.vacation.Definitions;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.VacationStressor;
import org.radargun.stamp.vacation.domain.Manager;
import org.radargun.stamp.vacation.transaction.DeleteCustomerOperation;
import org.radargun.stamp.vacation.transaction.MakeReservationOperation;
import org.radargun.stamp.vacation.transaction.UpdateTablesOperation;
import org.radargun.stamp.vacation.transaction.VacationTransaction;
import org.radargun.state.MasterState;


public class VacationBenchmarkStage extends AbstractDistStage {

    private static final String SIZE_INFO = "SIZE_INFO";

    private transient CacheWrapper cacheWrapper;

    private transient VacationStressor[] vacationStressors;

    public static int THREADS;
    
    private int clients;
    private int localThreads;
    private int readOnly;
    private int number;
    private int queries;
    private int relations;
    private int transactions;	// actually it's time now
    private int user;
    boolean totalOrder;

    public void setReadOnly(int ro) {
	this.readOnly = ro;
    }
    
    @Override
    public DistStageAck executeOnSlave() {
	DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
	this.cacheWrapper = slaveState.getCacheWrapper();
	if (cacheWrapper == null) {
	    log.info("Not running test on this slave as the wrapper hasn't been configured.");
	    return result;
	}

	log.info("Starting VacationBenchmarkStage: " + this.toString());

	VacationStressor.CLIENTS = clients;
	VacationStressor.MY_NODE = cacheWrapper.getMyNode();
	THREADS = localThreads;
	vacationStressors = new VacationStressor[localThreads];

	for (int t = 0; t < vacationStressors.length; t++) {
	    int numQueryPerTransaction = number;
	    int percentUser = user;

	    vacationStressors[t] = new VacationStressor();
	    vacationStressors[t].setQueryPerTx(numQueryPerTransaction);
	    vacationStressors[t].setPercentUser(percentUser);
	    vacationStressors[t].setQueryRange(queries);
	    vacationStressors[t].setReadOnlyPerc(this.readOnly);
	    vacationStressors[t].setCacheWrapper(cacheWrapper);
	    vacationStressors[t].setClients(getActiveSlaveCount());
	    vacationStressors[t].setThreadid(t);
	    vacationStressors[t].setRelations(relations);
	    vacationStressors[t].setTotalOrder(this.totalOrder);
	}

	try {
	    Thread[] workers = new Thread[vacationStressors.length];
	    for (int t = 0; t < workers.length; t++) {
		workers[t] = new Thread(vacationStressors[t]);
	    }
	    for (int t = 0; t < workers.length; t++) {
		workers[t].start();
	    }
	    try {
		Thread.sleep(transactions);
	    } catch (InterruptedException e) { }
	    for (int t = 0; t < workers.length; t++) {
		vacationStressors[t].setPhase(VacationStressor.SHUTDOWN_PHASE);
	    }
	    for (int t = 0; t < workers.length; t++) {
		workers[t].join();
	    }
	    Map<String, String> results = new LinkedHashMap<String, String>();
	    String sizeInfo = "size info: " + cacheWrapper.getInfo() +
		    ", clusterSize:" + super.getActiveSlaveCount() +
		    ", nodeIndex:" + super.getSlaveIndex() +
		    ", cacheSize: " + cacheWrapper.getCacheSize();
	    results.put(SIZE_INFO, sizeInfo);
	    long aborts = 0L;
	    long throughput = 0L;
	    for (int t = 0; t < workers.length; t++) {
		aborts += vacationStressors[t].getRestarts();
		throughput += vacationStressors[t].getThroughput();
	    }
	    results.put("THROUGHPUT", (((throughput + 0.0) * 1000) / transactions) + "");
	    results.put("TOTAL_RESTARTS", aborts + "");
	    log.info(sizeInfo);
	    result.setPayload(results);
	    return result;
	} catch (Exception e) {
	    log.warn("Exception while initializing the test", e);
	    result.setError(true);
	    result.setRemoteException(e);
	    return result;
	}
    }

    public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
	logDurationInfo(acks);
	boolean success = true;
	Map<Integer, Map<String, Object>> results = new HashMap<Integer, Map<String, Object>>();
	masterState.put("results", results);
	for (DistStageAck ack : acks) {
	    DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
	    if (wAck.isError()) {
		success = false;
		log.warn("Received error ack: " + wAck);
	    } else {
		if (log.isTraceEnabled())
		    log.trace(wAck);
	    }
	    Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
	    if (benchResult != null) {
		results.put(ack.getSlaveIndex(), benchResult);
		Object reqPerSes = benchResult.get("THROUGHPUT");
		if (reqPerSes == null) {
		    throw new IllegalStateException("This should be there!");
		}
		log.info("On slave " + ack.getSlaveIndex() + " it took " + (Double.parseDouble(reqPerSes.toString()) / 1000.0) + " seconds");
		log.info("Received " +  benchResult.remove(SIZE_INFO));
	    } else {
		log.trace("No report received from slave: " + ack.getSlaveIndex());
	    }
	}
	return success;
    }

    public CacheWrapper getCacheWrapper() {
	return cacheWrapper;
    }

    public void setCacheWrapper(CacheWrapper cacheWrapper) {
	this.cacheWrapper = cacheWrapper;
    }

    public int getClients() {
	return clients;
    }

    public void setClients(int clients) {
	this.clients = clients;
    }

    public int getLocalThreads() {
	return localThreads;
    }

    public void setLocalThreads(int localThreads) {
	THREADS = localThreads;
	this.localThreads = localThreads;
    }

    public int getNumber() {
	return number;
    }

    public void setNumber(int number) {
	this.number = number;
    }

    public int getQueries() {
	return queries;
    }

    public void setQueries(int queries) {
	this.queries = queries;
    }

    public int getRelations() {
	return relations;
    }

    public void setRelations(int relations) {
	this.relations = relations;
    }

    public int getTransactions() {
	return transactions;
    }

    public void setTransactions(int transactions) {
	this.transactions = transactions;
    }

    public int getUser() {
	return user;
    }

    public void setUser(int user) {
	this.user = user;
    }

    public static String getSizeInfo() {
	return SIZE_INFO;
    }
    public void setTotalOrder(boolean totalOrder) {
	this.totalOrder = totalOrder;
    }


}
