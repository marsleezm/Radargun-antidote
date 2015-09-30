package org.radargun.microbenchmark;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.domain.IntSet;
import org.radargun.microbenchmark.domain.IntSetLinkedList;
import org.radargun.microbenchmark.domain.IntSetRBTree;
import org.radargun.microbenchmark.domain.IntSetSkipList;
import org.radargun.microbenchmark.domain.IntSetTreeMap;

public class MicrobenchmarkPopulation {

    private static Log log = LogFactory.getLog(MicrobenchmarkPopulation.class);

    private final CacheWrapper wrapper;
    private final int items;
    private final int range;
    private final String set;
    private final int clients;

    public MicrobenchmarkPopulation(CacheWrapper wrapper, int items, int range, String set, int clients) {
	this.wrapper = wrapper;
	this.items = items;
	this.range = range;
	this.set = set;
	this.clients = clients;
    }

    public void performPopulation(){

	Random random = new Random();
	int n = wrapper.getMyNode();
	boolean successful = false;
	while (!successful) {
	    try {
		wrapper.startTransaction(false);

		IntSet mySet = null;
		if (set.equals("ll")) {
		    mySet = new IntSetLinkedList(n, wrapper);
		} else if (set.equals("sl")) {
		    mySet = new IntSetSkipList(n, wrapper);
		} else if (set.equals("rb")) {
		    mySet = new IntSetRBTree(n, wrapper);
		} else if (set.equals("tm")) {
		    mySet = new IntSetTreeMap(n, wrapper);
		} 

		for (int i = 0; i < items; i++)
		    mySet.add(wrapper, random.nextInt(range), true, false);

		for (int i = 0; i < 100; i++) {
		    LocatedKey key = wrapper.createKey("local" + n + "-" + i, n);
		    wrapper.put(null, key, 0);
		}
		
		LocatedKey key = wrapper.createKey("SET" + n, n);
		wrapper.put(null, key, mySet);
		
		wrapper.endTransaction(true);
		successful = true;
	    } catch (Throwable e) {
		System.out.println("Exception during population, going to rollback after this");
		e.printStackTrace();
		log.warn(e);
		try {
		    wrapper.endTransaction(false);
		} catch (Throwable e2) {
		    System.out.println("Exception during rollback!");
		    e2.printStackTrace();
		}
	    }
	}

	System.gc();
    }

}
