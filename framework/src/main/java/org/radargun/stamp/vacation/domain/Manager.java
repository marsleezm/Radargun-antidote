package org.radargun.stamp.vacation.domain;

/* =============================================================================
 *
 * manager.c
 * -- Travel reservation resource manager
 *
 * =============================================================================
 *
 * Copyright (C) Stanford University, 2006.  All Rights Reserved.
 * Author: Chi Cao Minh
 *
 * =============================================================================
 *
 * For the license of bayes/sort.h and bayes/sort.c, please see the header
 * of the files.
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of kmeans, please see kmeans/LICENSE.kmeans
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of ssca2, please see ssca2/COPYRIGHT
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of lib/mt19937ar.c and lib/mt19937ar.h, please see the
 * header of the files.
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of lib/rbtree.h and lib/rbtree.c, please see
 * lib/LEGALNOTICE.rbtree and lib/LICENSE.rbtree
 * 
 * ------------------------------------------------------------------------
 * 
 * Unless otherwise noted, the following license applies to STAMP files:
 * 
 * Copyright (c) 2007, Stanford University
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 * 
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 * 
 *     * Neither the name of Stanford University nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 *
 * =============================================================================
 */

/* =============================================================================
 * DECLARATION OF TM_CALLABLE FUNCTIONS
 * =============================================================================
 */

import java.io.Serializable;
import java.io.ObjectInputStream.GetField;
import java.util.Iterator;
import java.util.UUID;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.stamp.vacation.Cons;
import org.radargun.stamp.vacation.Definitions;
import org.radargun.stamp.vacation.OpacityException;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.VacationStressor;

public class Manager implements Serializable {
    public static final String CARS = "carTable";
    public static final String ROOMS = "roomsTable";
    public static final String FLIGHTS = "flightsTable";
    public static final String CUSTOMERS = "customersTable";
    public static final String NUMBER_RESOURCES = "numberResources";
    
    private int node;
    
    public Manager() { }
    
    public Manager(int node) {
	this.node = node;
    }
    
    public LocatedKey getNumberResourcesKey(CacheWrapper cache) {
        return cache.createKey(NUMBER_RESOURCES + ":manager:" + node, node);
    }
    
    public void putNumberResources(CacheWrapper cache, int number) {
        LocatedKey key =  cache.createKey(NUMBER_RESOURCES + ":manager:" + node, node);
        Vacation.put(cache, key, number);
    }
    
    int getNumberResources(CacheWrapper cache) {
        LocatedKey key =  cache.createKey(NUMBER_RESOURCES + ":manager:" + node, node);
        return (Integer) Vacation.get(cache, key);
    }

    void putCustomer(CacheWrapper cache, int id, Customer val) {
	LocatedKey key = cache.createKey(CUSTOMERS + ":" + id + ":" + node, node);
	Vacation.put(cache, key, val);
    }
    
    Customer getCustomer(CacheWrapper cache, int id) {
	LocatedKey key = cache.createKey(CUSTOMERS + ":" + id + ":" + node, node);
	return (Customer) Vacation.get(cache, key);
    }
    
    void putReservation(CacheWrapper cache, String table, int id, Reservation val) {
	LocatedKey key = cache.createKey(table + ":" + id + ":" + node, node);
	Vacation.put(cache, key, val);
    }
    
    Reservation getReservation(CacheWrapper cache, String table, int id) {
	LocatedKey key = cache.createKey(table + ":" + id + ":" + node, node);
	return (Reservation) Vacation.get(cache, key);
    }
    
    boolean addReservation(CacheWrapper cache, String table, String type, int id, int num, int price) {
	Reservation reservation = getReservation(cache, table, id);

	if (reservation == null) {
	    /* Create new reservation */
	    if (num < 1 || price < 0) {
		return false;
	    }
	    reservation = new Reservation(cache, type, id, num, price);
	    putReservation(cache, table, id, reservation);
	} else {
	    /* Update existing reservation */
	    if (!reservation.reservation_addToTotal(cache, num)) {
		return false;
	    }
	    if (reservation.getNumTotal(cache) == 0) {
	    } else {
		reservation.reservation_updatePrice(cache, price);
	    }
	}

	return true;
    }

    /*
     * ==========================================================================
     * === manager_addCar -- Add cars to a city -- Adding to an existing car
     * overwrite the price if 'price' >= 0 -- Returns TRUE on success, else
     * FALSE
     * ====================================================================
     * =========
     */
    public boolean manager_addCar(CacheWrapper cache, int carId, int numCars, int price) {
	return addReservation(cache, CARS, "car", carId, numCars, price);
    }

    /*
     * ==========================================================================
     * === manager_deleteCar -- Delete cars from a city -- Decreases available
     * car count (those not allocated to a customer) -- Fails if would make
     * available car count negative -- If decresed to 0, deletes entire entry --
     * Returns TRUE on success, else FALSE
     * ======================================
     * =======================================
     */
    public boolean manager_deleteCar(CacheWrapper cache, int carId, int numCar) {
	/* -1 keeps old price */
	return addReservation(cache, CARS, "car", carId, -numCar, -1);
    }

    /*
     * ==========================================================================
     * === manager_addRoom -- Add rooms to a city -- Adding to an existing room
     * overwrite the price if 'price' >= 0 -- Returns TRUE on success, else
     * FALSE
     * ====================================================================
     * =========
     */
    public boolean manager_addRoom(CacheWrapper cache, int roomId, int numRoom, int price) {
	return addReservation(cache, ROOMS, "room", roomId, numRoom, price);
    }

    /*
     * ==========================================================================
     * === manager_deleteRoom -- Delete rooms from a city -- Decreases available
     * room count (those not allocated to a customer) -- Fails if would make
     * available room count negative -- If decresed to 0, deletes entire entry
     * -- Returns TRUE on success, else FALSE
     * ====================================
     * =========================================
     */
    public boolean manager_deleteRoom(CacheWrapper cache, int roomId, int numRoom) {
	/* -1 keeps old price */
	return addReservation(cache, ROOMS, "room", roomId, -numRoom, -1);
    }

    /*
     * ==========================================================================
     * === manager_addFlight -- Add seats to a flight -- Adding to an existing
     * flight overwrite the price if 'price' >= 0 -- Returns TRUE on success,
     * FALSE on failure
     * ==========================================================
     * ===================
     */
    public boolean manager_addFlight(CacheWrapper cache, int flightId, int numSeat, int price) {
	return addReservation(cache, FLIGHTS, "flight", flightId, numSeat, price);
    }

    /*
     * ==========================================================================
     * === manager_deleteFlight -- Delete an entire flight -- Fails if customer
     * has reservation on this flight -- Returns TRUE on success, else FALSE
     * ====
     * =========================================================================
     */
    public boolean manager_deleteFlight(CacheWrapper cache, int flightId) {
	Reservation reservation = getReservation(cache, FLIGHTS, flightId);
	if (reservation == null) {
	    return false;
	}

	if (reservation.getNumUsed(cache) > 0) {
	    return false; /* somebody has a reservation */
	}

	return addReservation(cache, FLIGHTS, "flight", flightId, -reservation.getNumTotal(cache), -1);
    }

    /*
     * ==========================================================================
     * === manager_addCustomer -- If customer already exists, returns failure --
     * Returns TRUE on success, else FALSE
     * ======================================
     * =======================================
     */
    public boolean manager_addCustomer(CacheWrapper cache, int customerId) {
	Customer customer = getCustomer(cache, customerId);

	if (customer != null) {
	    return false;
	}

	customer = new Customer(cache, customerId);
	putCustomer(cache, customerId, customer);

	return true;
    }

    private String translateToTree(int type) {
	if (type == Definitions.RESERVATION_CAR) {
	    return CARS;
	} else if (type == Definitions.RESERVATION_FLIGHT) {
	    return FLIGHTS;
	} else if (type == Definitions.RESERVATION_ROOM) {
	    return ROOMS;
	}
	throw new RuntimeException("Did not find matching type for: " + type);
    }
    
    /*
     * ==========================================================================
     * === manager_deleteCustomer -- Delete this customer and associated
     * reservations -- If customer does not exist, returns success -- Returns
     * TRUE on success, else FALSE
     * ==============================================
     * ===============================
     */
    public boolean manager_deleteCustomer(CacheWrapper cache, int customerId) {
	Customer customer = getCustomer(cache, customerId);
	List_t<Reservation_Info> reservationInfoList;
	boolean status;

	if (customer == null) {
	    return false;
	}

	/* Cancel this customer's reservations */
	reservationInfoList = customer.reservationInfoList;

	Iterator<Reservation_Info> iter = reservationInfoList.iterator(cache);
	while (iter.hasNext()) {
	    Reservation_Info reservationInfo = iter.next();
	    Reservation reservation = getReservation(cache, translateToTree(reservationInfo.type), reservationInfo.id);
	    if (reservation == null) {
		throw new OpacityException();
	    }
	    status = reservation.reservation_cancel(cache);
	    if (!status) {
		throw new OpacityException();
	    }
	}

	return true;
    }

    /*
     * ==========================================================================
     * === QUERY INTERFACE
     * ======================================================
     * =======================
     */

    /*
     * ==========================================================================
     * === queryNumFree -- Return numFree of a reservation, -1 if failure
     * ========
     * =====================================================================
     */
    int queryNumFree(CacheWrapper cache, String table, int id) {
	int numFree = -1;
	Reservation reservation = getReservation(cache, table, id);
	if (reservation != null) {
	    numFree = reservation.getNumFree(cache);
	}

	return numFree;
    }

    /*
     * ==========================================================================
     * === queryPrice -- Return price of a reservation, -1 if failure
     * ============
     * =================================================================
     */
    int queryPrice(CacheWrapper cache, String table, int id) {
	int price = -1;
	Reservation reservation = getReservation(cache, table, id);
	if (reservation != null) {
	    price = reservation.getPrice(cache);
	}

	return price;
    }

    /*
     * ==========================================================================
     * === manager_queryCar -- Return the number of empty seats on a car --
     * Returns -1 if the car does not exist
     * ======================================
     * =======================================
     */
    public int manager_queryCar(CacheWrapper cache, int carId) {
	return queryNumFree(cache, CARS, carId);
    }

    /*
     * ==========================================================================
     * === manager_queryCarPrice -- Return the price of the car -- Returns -1 if
     * the car does not exist
     * ====================================================
     * =========================
     */
    public int manager_queryCarPrice(CacheWrapper cache, int carId) {
	return queryPrice(cache, CARS, carId);
    }

    /*
     * ==========================================================================
     * === manager_queryRoom -- Return the number of empty seats on a room --
     * Returns -1 if the room does not exist
     * ====================================
     * =========================================
     */
    public int manager_queryRoom(CacheWrapper cache, int roomId) {
	return queryNumFree(cache, ROOMS, roomId);
    }

    /*
     * ==========================================================================
     * === manager_queryRoomPrice -- Return the price of the room -- Returns -1
     * if the room does not exist
     * ================================================
     * =============================
     */
    public int manager_queryRoomPrice(CacheWrapper cache, int roomId) {
	return queryPrice(cache, ROOMS, roomId);
    }

    /*
     * ==========================================================================
     * === manager_queryFlight -- Return the number of empty seats on a flight
     * -- Returns -1 if the flight does not exist
     * ================================
     * =============================================
     */
    public int manager_queryFlight(CacheWrapper cache, int flightId) {
	return queryNumFree(cache, FLIGHTS, flightId);
    }

    /*
     * ==========================================================================
     * === manager_queryFlightPrice -- Return the price of the flight -- Returns
     * -1 if the flight does not exist
     * ==========================================
     * ===================================
     */
    public int manager_queryFlightPrice(CacheWrapper cache, int flightId) {
	return queryPrice(cache, FLIGHTS, flightId);
    }

    /*
     * ==========================================================================
     * === manager_queryCustomerBill -- Return the total price of all
     * reservations held for a customer -- Returns -1 if the customer does not
     * exist
     * ====================================================================
     * =========
     */
    public int manager_queryCustomerBill(CacheWrapper cache, int customerId) {
	int bill = -1;
	Customer customer = getCustomer(cache, customerId);

	if (customer != null) {
	    bill = customer.customer_getBill(cache);
	}

	return bill;
    }

    /*
     * ==========================================================================
     * === RESERVATION INTERFACE
     * ================================================
     * =============================
     */

    /*
     * ==========================================================================
     * === reserve -- Customer is not allowed to reserve same (type, id)
     * multiple times -- Returns TRUE on success, else FALSE
     * ====================
     * =========================================================
     */
    boolean reserve(CacheWrapper cache, String table, int customerId, int id, int type) {
	Customer customer = getCustomer(cache, customerId);
	Reservation reservation = getReservation(cache, table, id);

	if (customer == null) {
	    return false;
	}

	if (reservation == null) {
	    return false;
	}

	if (!reservation.reservation_make(cache)) {
	    return false;
	}

	if (!customer.customer_addReservationInfo(cache, type, id, reservation.getPrice(cache))) {
	    /* Undo previous successful reservation */
	    boolean status = reservation.reservation_cancel(cache);
	    if (!status) {
		throw new OpacityException();
	    }
	    return false;
	}

	return true;
    }

    /*
     * ==========================================================================
     * === manager_reserveCar -- Returns failure if the car or customer does not
     * exist -- Returns TRUE on success, else FALSE
     * ==============================
     * ===============================================
     */
    public boolean manager_reserveCar(CacheWrapper cache, int customerId, int carId) {
	return reserve(cache, CARS, customerId, carId, Definitions.RESERVATION_CAR);
    }

    /*
     * ==========================================================================
     * === manager_reserveRoom -- Returns failure if the room or customer does
     * not exist -- Returns TRUE on success, else FALSE
     * ==========================
     * ===================================================
     */
    public boolean manager_reserveRoom(CacheWrapper cache, int customerId, int roomId) {
	return reserve(cache, ROOMS, customerId, roomId, Definitions.RESERVATION_ROOM);
    }

    /*
     * ==========================================================================
     * === manager_reserveFlight -- Returns failure if the flight or customer
     * does not exist -- Returns TRUE on success, else FALSE
     * ====================
     * =========================================================
     */
    public boolean manager_reserveFlight(CacheWrapper cache, int customerId, int flightId) {
	return reserve(cache, FLIGHTS, customerId, flightId, Definitions.RESERVATION_FLIGHT);
    }

    /*
     * ==========================================================================
     * === cancel -- Customer is not allowed to cancel multiple times -- Returns
     * TRUE on success, else FALSE
     * ==============================================
     * ===============================
     */
    boolean cancel(CacheWrapper cache, String table, int customerId, int id, int type) {
	Customer customer = getCustomer(cache, customerId);
	Reservation reservation = getReservation(cache, table, id);

	if (customer == null) {
	    return false;
	}

	if (reservation == null) {
	    return false;
	}

	if (!reservation.reservation_cancel(cache)) {
	    return false;
	}

	if (!customer.customer_removeReservationInfo(cache, type, id)) {
	    /* Undo previous successful cancellation */
	    boolean status = reservation.reservation_make(cache);
	    if (!status) {
		throw new OpacityException();
	    }
	    return false;
	}

	return true;
    }

    /*
     * ==========================================================================
     * === manager_cancelCar -- Returns failure if the car, reservation, or
     * customer does not exist -- Returns TRUE on success, else FALSE
     * ============
     * =================================================================
     */
    boolean manager_cancelCar(CacheWrapper cache, int customerId, int carId) {
	return cancel(cache, CARS, customerId, carId, Definitions.RESERVATION_CAR);
    }

    /*
     * ==========================================================================
     * === manager_cancelRoom -- Returns failure if the room, reservation, or
     * customer does not exist -- Returns TRUE on success, else FALSE
     * ============
     * =================================================================
     */
    boolean manager_cancelRoom(CacheWrapper cache, int customerId, int roomId) {
	return cancel(cache, ROOMS, customerId, roomId, Definitions.RESERVATION_ROOM);
    }

    /*
     * ==========================================================================
     * === manager_cancelFlight -- Returns failure if the flight, reservation,
     * or customer does not exist -- Returns TRUE on success, else FALSE
     * ========
     * =====================================================================
     */
    boolean manager_cancelFlight(CacheWrapper cache, int customerId, int flightId) {
	return cancel(cache, FLIGHTS, customerId, flightId, Definitions.RESERVATION_FLIGHT);
    }
    
    public void manager_doCustomer(CacheWrapper wrapper) {
	LocatedKey key = wrapper.createKey("local" + Vacation.NODE_TARGET.get() + "-" + (((Vacation.NODE_TARGET.get() + 1) * 10000) + ((VacationStressor.MY_NODE + 1) * 100) + (VacationStressor.THREADID.get() + 1)), Vacation.NODE_TARGET.get());
	Vacation.put(wrapper, key, 1);
    }
}
