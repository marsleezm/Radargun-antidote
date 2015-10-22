package org.radargun.cachewrappers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.IDelayedComputation;
import org.radargun.LocatedKey;
import org.radargun.utils.TypedProperties;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AntidoteWrapper implements CacheWrapper {
   private static final Object NOT_IN_TRANSACTION = null;

   private static Log log = LogFactory.getLog(AntidoteWrapper.class);
   static List<TransactionManager> tms = new ArrayList<TransactionManager>();

   boolean started = false;
   String config;
   Method isPassiveReplicationMethod = null;

   private int numThreads = 1;

   public void setUp(String config, boolean isLocal, int nodeIndex, TypedProperties confAttributes) throws Exception {
      this.config = config;
      String configFile  = confAttributes.containsKey("file") ? confAttributes.getProperty("file") : config;
      String cacheName = confAttributes.containsKey("cache") ? confAttributes.getProperty("cache") : "x";

      log.trace("Using config file: " + configFile + " and cache name: " + cacheName);

      if (!started) {
         started = true;
         DCInfoManager.init();
         while(tms.size() < numThreads)
 			tms.add(new TransactionManager());
         log.info("DCInfo just started!");
      }
      log.info("Using config attributes: " + confAttributes);
   }
   
   @Override
    public void clusterFormed(int expected) {
           while (DCInfoManager.getAddressesSize() != expected) {
               try {
                   Thread.sleep(1000);
               } catch (InterruptedException e) {}
              
           }
           MagicKey.NODE_INDEX = DCInfoManager.getNodeIndex();
    }
   
   @Override
    public int getMyNode() {
        return DCInfoManager.getNodeIndex();
    }
   
   @Override
    public LocatedKey createKey(String key, int node) {
        return new MagicKey(key, node);
    }
   
   public void tearDown() throws Exception {
	   
      String addressList = DCInfoManager.getMembers().toString();
      if (started) {
    	 for(TransactionManager tm : tms)
    		 tm.stopConnections();
         log.trace("Stopped, previous view is " + addressList);
         started = false;
      }
   }

   public void put(String bucket, Object key, Object value) throws Exception {
	  if (bucket == null)
		  getThreadLocalTm().put(key, value);
	  else
		  getThreadLocalTm().put(bucket+key, value);
   }

   @Override
   public void putIfLocal(String bucket, Object key, Object value) throws Exception {
	   if (bucket == null)
		   getThreadLocalTm().put(key, value);
	   else
	       getThreadLocalTm().put(bucket+key, value);
   }

   public Object get(String bucket, Object key) throws Exception {
	  String threadName = Thread.currentThread().getName();
	  String[] substrs = threadName.split("-");
	  Integer threadId = new Integer(substrs[substrs.length-1]);
	  TransactionManager tm = tms.get(threadId % numThreads);
	  if (bucket == null)
		  return tm.get(threadId, key);
	  else
		  return tm.get(threadId, bucket+key);
   }

   public void empty() throws Exception {
      //use keySet().size() rather than size directly as cache.size might not be reliable
      //log.info("Cache size before clear (cluster size= " + DCInfoManager.getAddressesSize() +")" 
    //		  	+ DCInfoManager.getCacheSize());

     // for(TransactionManager tm : tms)
    //	  tm.stopConnections();
     // DCInfoManager.clear();
      log.info("Not really emptying the cache");
   }

   public int getNumMembers() {
      if (DCInfoManager.getMembers() != null) {
         log.trace("Members are: " + DCInfoManager.getMembers());
      }
      return DCInfoManager.getMembers() == null ? 0 : DCInfoManager.getMembers().size();
   }

   public String getInfo() {
      //Important: don't change this string without validating the ./dist.sh as it relies on its format!!
      return "Running : " + "1.0.0" +  ", config:" + config + ", cacheName:" + "Antidote";
   }

   public Object getReplicatedData(String bucket, String key) throws Exception {
      return get(bucket, key);
   }

   @Override
   public void startTransaction(boolean isReadOnly) {
	  TransactionManager tm = getThreadLocalTm();
      assertTm(tm);
      try {
         tm.begin();
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void endTransaction(boolean successful) {
	  String threadName = Thread.currentThread().getName();
	  String[] substrs = threadName.split("-");
	  Integer threadId = new Integer(substrs[substrs.length-1]);
	  TransactionManager tm = tms.get(threadId % numThreads);
      assertTm(tm);
      if (successful){
    	  if( tm.commit(threadId) == false)
    		  throw new RuntimeException();
      }
      else{
          tm.abort();
      }
   }

   @Override
   public boolean isInTransaction() {
	   TransactionManager tm = getThreadLocalTm();
       return tm != null && tm.getStatus() != NOT_IN_TRANSACTION;
   }


   private void assertTm(TransactionManager tm) {
      if (tm == null) throw new RuntimeException("No configured TM!");
   }

   public void setEnlistExtraXAResource(boolean enlistExtraXAResource) {
   }

   @Override
   public int getCacheSize() {
      return DCInfoManager.getCacheSize();
   }

   @Override
   public Map<String, String> getAdditionalStats() {
      return new HashMap<String, String>();
   }

   @Override
   public boolean isPassiveReplication() {
      return true;
   }

   @Override
   public boolean isTheMaster() {
      return !isPassiveReplication() || getThreadLocalTm().isCoordinator();
   }
   
   private synchronized TransactionManager getThreadLocalTm() {
	   String threadName = Thread.currentThread().getName();
	   String[] substrs = threadName.split("-");
	   Integer index = new Integer(substrs[substrs.length-1]);
	   return tms.get(index % numThreads);
   }
   
   //================================================= JMX STATS ====================================================

   public void setupTotalOrder() {
       //CustomHashing.totalOrder = true;
   }

   @Override
   public Object getDelayed(Object key) {
       return getThreadLocalTm().delayedGet(key);
   }

   @Override
   public void putDelayed(Object key, Object value) {
	   getThreadLocalTm().delayedPut(key, value);
   }

   @Override
   public void resetAdditionalStats() {
	   // TODO Auto-generated method stub
   }

   @Override
   public void delayComputation(IDelayedComputation<?> computation) {
	   // TODO Auto-generated method stub
   }

   public static void clearTMs()
   {
	   for(TransactionManager tm : tms)
		 tm.stopConnections();
   }

	@Override
	public void setParallelism(int numThreads) {
		this.numThreads  = numThreads;
		while(tms.size() < numThreads){
			log.info("Increasing tms: now num is "+tms.size());
			tms.add(new TransactionManager());
		}
	}
}
