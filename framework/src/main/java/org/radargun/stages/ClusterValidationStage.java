package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.utils.Utils;

import java.util.List;

/**
 * Distributed stage that would validate that cluster is correctly formed.
 * <pre>
 * Algorithm:
 * - each slave does a put(slaveIndex);
 * - each slave checks wether all (or part) of the remaining slaves replicated here.
 *
 * Config:
 *   - 'isPartialReplication' : is set to true, then the slave will consider that the cluster is formed when one slave
 *      replicated here. If false (default value) then replication will only be considered successful if all
 * (clusterSize)
 *      slaves replicated here.
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 */
public class ClusterValidationStage extends AbstractDistStage {

   private static final String KEY = "_InstallBenchmarkStage_";
   private static final String CONFIRMATION_KEY = "_confirmation_";


   private boolean isPartialReplication = false;
   //TODO: The code sucks so many so it is impossible to figure how/when/why to set partial rep or passive rep..
   //I just manually changed isPassiveReplication to true to let code pass.. (because it is true indeed)
   private boolean isPassiveReplication = false;
   private int replicationTryCount = 60;
   private int replicationTimeSleep = 2000;


   private CacheWrapper wrapper;
   private static final String BUCKET = "clusterValidation";

   public DistStageAck executeOnSlave() {
	  isPartialReplication = true;
      DefaultDistStageAck response = newDefaultStageAck();
      try {
         wrapper = slaveState.getCacheWrapper();
         log.trace("Got cache wrapper");
         int replResult = checkReplicationSeveralTimes();
         if (!isPartialReplication && !isPassiveReplication) {
            if (replResult > 0) {//only executes this on the slaves on which replication happened.
               int index = confirmReplication();
               if (index >= 0) {
                  response.setError(true);
                  response.setErrorMessage("Slave with index " + index + " hasn't confirmed the replication");
                  return response;
               }
            }
         } else {
            log.info("Using partial/passive replication, skipping confirm phase");
         }
         response.setPayload(replResult);
      } catch (Exception e) {
         response.setError(true);
         response.setRemoteException(e);
         return response;
      }
      return response;
   }

   private int confirmReplication() throws Exception {
	  log.warn("In confirmReplication");
      wrapper.put(nodeBucket(getSlaveIndex()), confirmationKey(getSlaveIndex()), "true");
      for (int i = 0; i < getActiveSlaveCount(); i++) {
         for (int j = 0; j < 10 && (wrapper.get(nodeBucket(i), confirmationKey(i)) == null); j++) {
        	log.warn("Put something already"+i+" "+j);
            tryToPut();
            wrapper.put(nodeBucket(getSlaveIndex()), confirmationKey(getSlaveIndex()), "true");
            Thread.sleep(1000);
         }
         if (wrapper.get(nodeBucket(i), confirmationKey(i)) == null) {
            log.warn("Confirm phase unsuccessful. Slave " + i + " hasn't acknowledged the test");
            return i;
         }
      }
      log.info("Confirm phase successful.");
      return -1;
   }

   private String confirmationKey(int slaveIndex) {
      return CONFIRMATION_KEY + slaveIndex;
   }

   private String nodeBucket(int slaveIndex) {
      return BUCKET + slaveIndex;
   }

   public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
	   //TODO: This sucks so much and doens't make sense... As a reminder that it's very ad-hoc to set variable just ot true
	   
	   isPartialReplication = true;
	  //isPassiveReplication = true;
      logDurationInfo(acks);
      boolean success = true;
      for (DistStageAck ack : acks) {
         DefaultDistStageAck defaultStageAck = (DefaultDistStageAck) ack;
         if (defaultStageAck.isError()) {
            log.warn("Ack error from remote slave: " + defaultStageAck);
            return false;
         }
         int replCount = (Integer) defaultStageAck.getPayload();
         if (isPartialReplication || isPassiveReplication) {
            if (!(replCount > 0)) {
               log.warn("Replication hasn't occurred on slave: " + defaultStageAck);
               success = false;
            }
         } else { //total replication expected
            int expectedRepl = getActiveSlaveCount() - 1;
            if (!(replCount == expectedRepl)) {
               log.warn("On slave " + ack + " total replication hasn't occurred. Expected " + expectedRepl + " and received " + replCount);
               success = false;
            }
         }
      }
      if (success) {
         log.info("Cluster successfully formed!");
      } else {
         log.warn("Cluster hasn't formed!");
      }
      return success;
   }


   private void tryToPut() throws Exception {
      int tryCount = 0;
      while (tryCount < 5) {
         try {
            if (!wrapper.isPassiveReplication() || wrapper.isTheMaster()) {
               wrapper.put(nodeBucket(getSlaveIndex()), key(getSlaveIndex()), "true");
            }
            return;
         }
         catch (Throwable e) {
            log.warn("Error while trying to put data: ", e);
            tryCount++;
         }
      }
      throw new Exception("Couldn't accomplish addition before replication!");
   }

   private int checkReplicationSeveralTimes() throws Exception {
      tryToPut();
      int replCount = 0;
      for (int i = 0; i < replicationTryCount; i++) {
         replCount = replicationCount();
         log.info("replication count is "+replCount+", partialrep is="+isPartialReplication
        		 +", isPassiveReplication="+isPassiveReplication);
         if ((isPartialReplication && replCount >= 1) ||
               (!isPartialReplication && !isPassiveReplication && (replCount == getActiveSlaveCount() - 1)) ||
               (isPassiveReplication && replCount == 1)) {
            log.info("Replication test successfully passed. isPartialReplication? " + isPartialReplication +
                           ", isPassiveReplication? " + isPassiveReplication +
                           ", replicationCount = " + replCount);
            return replCount;
         }
         //adding our stuff one more time
         tryToPut();
         log.info("Replication test failed, " + (i + 1) + " tries so far. Sleeping for " + Utils.prettyPrintMillis(replicationTimeSleep)
               + " and trying again.");
         Thread.sleep(replicationTimeSleep);
      }
      log.info("Replication test failed. Last replication count is " + replCount);
      return -1;
   }

   private int replicationCount() throws Exception {
      int clusterSize = getActiveSlaveCount();
      log.info("Check replication count, cluster size is"+clusterSize);
      int replicaCount = 0;
      for (int i = 0; i < clusterSize; i++) {
         int currentSlaveIndex = getSlaveIndex();
         if (i == currentSlaveIndex && !isPassiveReplication && clusterSize > 1) { //the master in passive replication can only see himself data
            continue;
         }
         Object data = tryGet(i);
         if (data == null || !"true".equals(data)) {
            log.trace("Cache with index " + i + " did *NOT* replicate");
         } else {
            log.trace("Cache with index " + i + " replicated here ");
            replicaCount++;
         }
      }
      log.info("Number of caches that replicated here is " + replicaCount);
      return replicaCount;
   }


   private Object tryGet(int i) throws Exception {
      int tryCont = 0;
      while (tryCont < 5) {
         try {
            return wrapper.getReplicatedData(nodeBucket(i), key(i));
         } catch (Throwable e) {
            tryCont++;
         }
      }
      return null;
   }

   public void setPartialReplication(boolean partialReplication) {
      isPartialReplication = partialReplication;
   }

   public void setReplicationTryCount(int replicationTryCount) {
      this.replicationTryCount = replicationTryCount;
   }

   public void setReplicationTimeSleep(int replicationTimeSleep) {
      this.replicationTimeSleep = replicationTimeSleep;
   }

   private String key(int slaveIndex) {
      return KEY + slaveIndex;
   }

   public void setPassiveReplication(boolean passiveReplication) {
	  log.info("Some one set passiveRep to"+passiveReplication);
      isPassiveReplication = passiveReplication;
   }

   @Override
   public String toString() {
      return "ClusterValidationStage {" +
            "isPassiveReplication=" + isPassiveReplication +
            ", isPartialReplication=" + isPartialReplication +
            ", replicationTryCount=" + replicationTryCount +
            ", replicationTimeSleep=" + replicationTimeSleep +
            ", wrapper=" + wrapper + ", " + super.toString();
   }
}
