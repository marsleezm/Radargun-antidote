package org.radargun.tpcw.transaction;

public interface TPCWPartialInteraction<T> {

    public T interact() throws Throwable;
    
}
