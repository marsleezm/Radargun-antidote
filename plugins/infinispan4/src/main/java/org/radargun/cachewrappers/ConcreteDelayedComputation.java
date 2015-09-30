package org.radargun.cachewrappers;

import java.io.Serializable;
import java.util.Collection;

import org.infinispan.DelayedComputation;
import org.radargun.IDelayedComputation;

public class ConcreteDelayedComputation<T> implements DelayedComputation<T>, Serializable {

    private IDelayedComputation<T> comp;
    
    public ConcreteDelayedComputation(IDelayedComputation<T> comp) {
        this.comp = comp;
    }
    
    @Override
    public Collection<Object> getAffectedKeys() {
        return this.comp.getIAffectedKeys();
    }

    @Override
    public T compute() {
        return this.comp.computeI();
    }

}
