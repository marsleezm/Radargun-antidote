package org.radargun.tpcw;

import org.radargun.CacheWrapper;

public interface DomainObject {

   void store(CacheWrapper wrapper) throws Throwable;

   void store(CacheWrapper wrapper, int nodeIndex) throws Throwable;

   void storeToPopulate(CacheWrapper wrapper, int nodeIndex, boolean localOnly) throws Throwable;

   boolean load(CacheWrapper wrapper) throws Throwable;
}
