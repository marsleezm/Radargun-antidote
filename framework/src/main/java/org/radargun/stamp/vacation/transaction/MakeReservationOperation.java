package org.radargun.stamp.vacation.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.stages.VacationBenchmarkStage;
import org.radargun.stamp.vacation.Definitions;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.VacationStressor;
import org.radargun.stamp.vacation.domain.Manager;

public class MakeReservationOperation extends VacationTransaction {

    final private int[] types;
    final private int[] ids;
    final private int[] maxPrices;
    final private int[] maxIds;
    final private int customerId;
    final private int numQuery;
    final private boolean readOnly;
    final private boolean local;
    final private boolean totalOrder;

    public MakeReservationOperation(Random random, int numQueryPerTx, int queryRange, int relations, int readOnly, boolean totalOrder) {
	super(random.random_generate(), queryRange);
	this.types = new int[numQueryPerTx];
	this.ids = new int[numQueryPerTx];

	this.maxPrices = new int[Definitions.NUM_RESERVATION_TYPE];
	this.maxIds = new int[Definitions.NUM_RESERVATION_TYPE];
	this.maxPrices[0] = -1;
	this.maxPrices[1] = -1;
	this.maxPrices[2] = -1;
	this.maxIds[0] = -1;
	this.maxIds[1] = -1;
	this.maxIds[2] = -1;
	int n;
	this.numQuery = numQueryPerTx;

	int base = (relations * VacationStressor.THREADID.get()) / VacationBenchmarkStage.THREADS;
	int parcel = relations / VacationBenchmarkStage.THREADS;
	customerId = base + (random.posrandom_generate() % parcel);
	for (n = 0; n < numQuery; n++) {
            types[n] = random.random_generate() % Definitions.NUM_RESERVATION_TYPE;
            ids[n] = base + (random.random_generate() % parcel);
        }

	this.readOnly = (random.random_generate() % 100) <= readOnly;
	this.local = (random.random_generate() % 100) <= readOnly;
        if (this.readOnly) {
            super.node = VacationStressor.MY_NODE;
        }
        
        this.totalOrder = totalOrder;
    }

    @Override
    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
	Vacation.NODE_TARGET.set(super.node);
	LocatedKey key = cacheWrapper.createKey("MANAGER" + super.node, super.node);
	Manager manager = (Manager) cacheWrapper.get(null, key);
	boolean isFound = false;
	int n;
	for (n = 0; n < numQuery; n++) {
//	    if (totalOrder && remote) {
//		if (n % 2 == 0) {
//		    int otherNode = (super.node + 1) % VacationStressor.CLIENTS;
//		    Vacation.NODE_TARGET.set(otherNode);
//		    key = cacheWrapper.createKey("MANAGER" + otherNode, otherNode);
//		    manager = (Manager) cacheWrapper.get(null, key);		    
//		} else {
//		    Vacation.NODE_TARGET.set(super.node);
//		    key = cacheWrapper.createKey("MANAGER" + super.node, super.node);
//		    manager = (Manager) cacheWrapper.get(null, key);
//		}
//	    }
	    int t = types[n];
	    int id = ids[n];
	    int price = -1;
	    if (t == Definitions.RESERVATION_CAR) {
		if (manager.manager_queryCar(cacheWrapper, id) >= 0) {
		    price = manager.manager_queryCarPrice(cacheWrapper, id);
		}
	    } else if (t == Definitions.RESERVATION_FLIGHT) {
		if (manager.manager_queryFlight(cacheWrapper, id) >= 0) {
		    price = manager.manager_queryFlightPrice(cacheWrapper, id);
		}
	    } else if (t == Definitions.RESERVATION_ROOM) {
		if (manager.manager_queryRoom(cacheWrapper, id) >= 0) {
		    price = manager.manager_queryRoomPrice(cacheWrapper, id);
		}
	    } else {
		assert (false);
	    }
	    if (price > maxPrices[t]) {
		maxPrices[t] = price;
		maxIds[t] = id;
		isFound = true;
	    }
	}

	if (!readOnly) {
	    if (!remote) {
		if (isFound) {
		    manager.manager_addCustomer(cacheWrapper, customerId);
		}
		if (maxIds[Definitions.RESERVATION_CAR] > 0) {
		    manager.manager_reserveCar(cacheWrapper, customerId, maxIds[Definitions.RESERVATION_CAR]);
		}
		if (maxIds[Definitions.RESERVATION_FLIGHT] > 0) {
		    manager.manager_reserveFlight(cacheWrapper, customerId, maxIds[Definitions.RESERVATION_FLIGHT]);
		}
		if (maxIds[Definitions.RESERVATION_ROOM] > 0) {
		    manager.manager_reserveRoom(cacheWrapper, customerId, maxIds[Definitions.RESERVATION_ROOM]);
		}
	    }
	    manager.manager_doCustomer(cacheWrapper);
	}
    }

    @Override
    public boolean isReadOnly() {
	return this.readOnly;
    }

}
