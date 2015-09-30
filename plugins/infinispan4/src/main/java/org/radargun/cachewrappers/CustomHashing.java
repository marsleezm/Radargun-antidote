package org.radargun.cachewrappers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;

public class CustomHashing extends DefaultConsistentHash {

    public static boolean totalOrder = false;

    private volatile Address[] addresses;

    public int getAddressesSize() {
	return addresses.length;
    }

    @Override
    public void setCaches(Set<Address> newCaches) {
	super.setCaches(newCaches);
	addresses = new Address[newCaches.size()];
	int i = 0;
	for (Address addr : newCaches) {
	    addresses[i] = addr;
	    i++;
	}
    }

    @Override
    public List<Address> locate(Object key, int replCount) {
	final int actualReplCount = Math.min(replCount, caches.size());
	if (key instanceof MagicKey) {
	    List<Address> result = new ArrayList<Address>(actualReplCount);
	    int node = ((MagicKey)key).node + getOffset((MagicKey) key);
	    for (int i = 0; i < actualReplCount; i++) {
		result.add(addresses[(node + i) % addresses.length]);
	    }
	    return result;
	} else {
	    return super.locate(key, replCount); 
	}
    }

    @Override
    public boolean isKeyLocalToAddress(Address target, Object key, int replCount) {
	final int actualReplCount = Math.min(replCount, caches.size());
	if (key instanceof MagicKey) {
	    int node = ((MagicKey)key).node + getOffset((MagicKey) key);
	    for (int i = 0; i < actualReplCount; i++) {
		if (target.equals(addresses[(node + i) % addresses.length])) {
		    return true;
		}
	    }
	    return false;
	} else {
	    return super.isKeyLocalToAddress(target, key, replCount);
	}
    }

    private int getOffset(MagicKey key) {
//	if (totalOrder) {
//	    int hashCode = key.key.hashCode();
//	    return Math.abs(Math.abs(hashCode) % 2);
//	} else {
	    return 0;
//	}
    }
    
    @Override
    public Address primaryLocation(Object key) {
	if (key instanceof MagicKey) {
	    return addresses[(((MagicKey)key).node + getOffset((MagicKey) key)) % addresses.length];
	} else {
	    return super.primaryLocation(key);
	}
    }

    public int getMyId(Address addr) {
	for (int i = 0; i < addresses.length; i++) {
	    if (addresses[i].equals(addr)) {
		return i;
	    }
	}
	throw new RuntimeException("Could not find addr: " + addr);
    }
}
