package org.radargun.cachewrappers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.IDelayedComputation;
import org.radargun.LocatedKey;
import org.radargun.utils.TypedProperties;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class AntidoteWrapper implements CacheWrapper {
   private static final Object NOT_IN_TRANSACTION = null;

   private static Log log = LogFactory.getLog(AntidoteWrapper.class);
   TransactionManager tm;

   boolean started = false;
   String config;
   Method isPassiveReplicationMethod = null;

   public void setUp(String config, boolean isLocal, int nodeIndex, TypedProperties confAttributes) throws Exception {
      this.config = config;
      String configFile  = confAttributes.containsKey("file") ? confAttributes.getProperty("file") : config;
      String cacheName = confAttributes.containsKey("cache") ? confAttributes.getProperty("cache") : "x";

      log.trace("Using config file: " + configFile + " and cache name: " + cacheName);

      log.warn("Finishing loading config file:"+config);
      if (!started) {
         DCManager.init(config);
         started = true;
         tm = new TransactionManager();
         log.info("Using transaction manager: " + tm);
      }
      log.info("Using config attributes: " + confAttributes);
   }
   
   @Override
    public void clusterFormed(int expected) {
           while (DCManager.getAddressesSize() != expected) {
               try {
                   Thread.sleep(1000);
               } catch (InterruptedException e) {}
              
           }
           MagicKey.NODE_INDEX = DCManager.getNodeIndex();
    }
   
   @Override
    public int getMyNode() {
        return MagicKey.NODE_INDEX;
    }
   
   @Override
    public LocatedKey createKey(String key, int node) {
        return new MagicKey(key, node);
    }
   
   public void tearDown() throws Exception {
      List<String> addressList = DCManager.getMembers();
      if (started) {
    	 DCManager.stop();
         log.trace("Stopped, previous view is " + addressList);
         started = false;
      }
   }

   public void put(String bucket, Object key, Object value) throws Exception {
      tm.put(key, value);
   }

   @Override
   public void putIfLocal(String bucket, Object key, Object value) throws Exception {
      tm.put(key, value);
   }

   public Object get(String bucket, Object key) throws Exception {
      return tm.get(key);
   }

   public void empty() throws Exception {
      //use keySet().size() rather than size directly as cache.size might not be reliable
      log.info("Cache size before clear (cluster size= " + DCManager.getAddressesSize() +")" 
    		  	+ DCManager.getCacheSize());

      DCManager.clear();
      log.info("Cache size after clear: " + DCManager.getCacheSize());
   }

   public int getNumMembers() {
      if (DCManager.getMembers() != null) {
         log.trace("Members are: " + DCManager.getMembers());
      }
      return DCManager.getMembers() == null ? 0 : DCManager.getMembers().size();
   }

   public String getInfo() {
      //Important: don't change this string without validating the ./dist.sh as it relies on its format!!
      return "Running : " + DCManager.getVersion() +  ", config:" + config + ", cacheName:" + DCManager.getName();
   }

   public Object getReplicatedData(String bucket, String key) throws Exception {
      return get(bucket, key);
   }

   @Override
   public void startTransaction(boolean isReadOnly) {
      assertTm();
      try {
         tm.begin();
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void endTransaction(boolean successful) {
      assertTm();
      try {
         if (successful)
            tm.commit();
         else
            tm.abort();
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public boolean isInTransaction() {
         return tm != null && tm.getStatus() != NOT_IN_TRANSACTION;
   }


   private void assertTm() {
      if (tm == null) throw new RuntimeException("No configured TM!");
   }

   public void setEnlistExtraXAResource(boolean enlistExtraXAResource) {
   }

   @Override
   public int getCacheSize() {
      return DCManager.getCacheSize();
   }

   @Override
   public Map<String, String> getAdditionalStats() {
      return DCManager.getStat();
   }

   @Override
   public boolean isPassiveReplication() {
      return true;
   }

   @Override
   public boolean isTheMaster() {
      return !isPassiveReplication() || tm.isCoordinator();
   }
   
   //================================================= JMX STATS ====================================================

   public void setupTotalOrder() {
       //CustomHashing.totalOrder = true;
   }

   @Override
   public Object getDelayed(Object key) {
       return tm.delayedGet(key);
   }

   @Override
   public void putDelayed(Object key, Object value) {
       tm.delayedPut(key, value);
   }

   @Override
   public void resetAdditionalStats() {
	   // TODO Auto-generated method stub
   }

   @Override
   public void delayComputation(IDelayedComputation<?> computation) {
	   // TODO Auto-generated method stub
   }
}
