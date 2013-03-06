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
import java.util.Iterator;
import java.util.UUID;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.stamp.vacation.Definitions;
import org.radargun.stamp.vacation.OpacityException;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.VacationStressor;

public class Manager implements Serializable {
    /* final */ RBTree<Integer, Reservation> carTable;
    /* final */ RBTree<Integer, Reservation> roomTable;
    /* final */ RBTree<Integer, Reservation> flightTable;
    /* final */ RBTree<Integer, Customer> customerTable;

    public Manager() { }
    
    public Manager(CacheWrapper cache, boolean dummy) {
	carTable = new RBTree<Integer, Reservation>(cache, "carTable");
	roomTable = new RBTree<Integer, Reservation>(cache, "roomTable");
	flightTable = new RBTree<Integer, Reservation>(cache, "flightTable");
	customerTable = new RBTree<Integer, Customer>(cache, "customerTable");
    }

    boolean addReservation(CacheWrapper cache, RBTree<Integer, Reservation> table, String type, int id, int num, int price) {
	Reservation reservation;

	reservation = table.get(cache, id);
	if (reservation == null) {
	    /* Create new reservation */
	    if (num < 1 || price < 0) {
		return false;
	    }
	    reservation = new Reservation(cache, type, id, num, price);
	    table.put(cache, id, reservation);
	} else {
	    /* Update existing reservation */
	    if (!reservation.reservation_addToTotal(cache, num)) {
		return false;
	    }
	    if (reservation.getNumTotal(cache) == 0) {
		boolean status = table.remove(cache, id);
		if (!status) {
		    throw new OpacityException();
		}
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
	return addReservation(cache, carTable, "car", carId, numCars, price);
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
	return addReservation(cache, carTable, "car", carId, -numCar, -1);
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
	return addReservation(cache, roomTable, "room", roomId, numRoom, price);
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
	return addReservation(cache, roomTable, "room", roomId, -numRoom, -1);
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
	return addReservation(cache, flightTable, "flight", flightId, numSeat, price);
    }

    /*
     * ==========================================================================
     * === manager_deleteFlight -- Delete an entire flight -- Fails if customer
     * has reservation on this flight -- Returns TRUE on success, else FALSE
     * ====
     * =========================================================================
     */
    public boolean manager_deleteFlight(CacheWrapper cache, int flightId) {
	Reservation reservation = flightTable.get(cache, flightId);
	if (reservation == null) {
	    return false;
	}

	if (reservation.getNumUsed(cache) > 0) {
	    return false; /* somebody has a reservation */
	}

	return addReservation(cache, flightTable, "flight", flightId, -reservation.getNumTotal(cache), -1);
    }

    /*
     * ==========================================================================
     * === manager_addCustomer -- If customer already exists, returns failure --
     * Returns TRUE on success, else FALSE
     * ======================================
     * =======================================
     */
    public boolean manager_addCustomer(CacheWrapper cache, int customerId) {
	Customer customer;

	if (customerTable.get(cache, customerId) != null) {
	    return false;
	}

	customer = new Customer(cache, customerId);
	Customer oldCustomer = customerTable.putIfAbsent(cache, customerId, customer);
	if (oldCustomer != null) {
	    throw new OpacityException();
	}

	return true;
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
	Customer customer;
	RBTree<Integer, Reservation> reservationTables[] = new RBTree[Definitions.NUM_RESERVATION_TYPE];
	List_t<Reservation_Info> reservationInfoList;
	boolean status;

	customer = customerTable.get(cache, customerId);
	if (customer == null) {
	    return false;
	}

	reservationTables[Definitions.RESERVATION_CAR] = carTable;
	reservationTables[Definitions.RESERVATION_ROOM] = roomTable;
	reservationTables[Definitions.RESERVATION_FLIGHT] = flightTable;

	/* Cancel this customer's reservations */
	reservationInfoList = customer.reservationInfoList;

	Iterator<Reservation_Info> iter = reservationInfoList.iterator(cache);
	while (iter.hasNext()) {
	    Reservation_Info reservationInfo = iter.next();
	    Reservation reservation = reservationTables[reservationInfo.type].get(cache, reservationInfo.id);
	    if (reservation == null) {
		throw new OpacityException();
	    }
	    status = reservation.reservation_cancel(cache);
	    if (!status) {
		throw new OpacityException();
	    }
	}

	status = customerTable.remove(cache, customerId);
	if (!status) {
	    throw new OpacityException();
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
    int queryNumFree(CacheWrapper cache, RBTree<Integer, Reservation> table, int id) {
	int numFree = -1;
	Reservation reservation = table.get(cache, id);
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
    int queryPrice(CacheWrapper cache, RBTree<Integer, Reservation> table, int id) {
	int price = -1;
	Reservation reservation = table.get(cache, id);
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
	return queryNumFree(cache, carTable, carId);
    }

    /*
     * ==========================================================================
     * === manager_queryCarPrice -- Return the price of the car -- Returns -1 if
     * the car does not exist
     * ====================================================
     * =========================
     */
    public int manager_queryCarPrice(CacheWrapper cache, int carId) {
	return queryPrice(cache, carTable, carId);
    }

    /*
     * ==========================================================================
     * === manager_queryRoom -- Return the number of empty seats on a room --
     * Returns -1 if the room does not exist
     * ====================================
     * =========================================
     */
    public int manager_queryRoom(CacheWrapper cache, int roomId) {
	return queryNumFree(cache, roomTable, roomId);
    }

    /*
     * ==========================================================================
     * === manager_queryRoomPrice -- Return the price of the room -- Returns -1
     * if the room does not exist
     * ================================================
     * =============================
     */
    public int manager_queryRoomPrice(CacheWrapper cache, int roomId) {
	return queryPrice(cache, roomTable, roomId);
    }

    /*
     * ==========================================================================
     * === manager_queryFlight -- Return the number of empty seats on a flight
     * -- Returns -1 if the flight does not exist
     * ================================
     * =============================================
     */
    public int manager_queryFlight(CacheWrapper cache, int flightId) {
	return queryNumFree(cache, flightTable, flightId);
    }

    /*
     * ==========================================================================
     * === manager_queryFlightPrice -- Return the price of the flight -- Returns
     * -1 if the flight does not exist
     * ==========================================
     * ===================================
     */
    public int manager_queryFlightPrice(CacheWrapper cache, int flightId) {
	return queryPrice(cache, flightTable, flightId);
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
	Customer customer;

	customer = customerTable.get(cache, customerId);

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
    static boolean reserve(CacheWrapper cache, RBTree<Integer, Reservation> table, RBTree<Integer, Customer> customerTable, int customerId, int id, int type) {
	Customer customer;
	Reservation reservation;

	customer = customerTable.get(cache, customerId);

	if (customer == null) {
	    return false;
	}

	reservation = table.get(cache, id);
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
	return reserve(cache, carTable, customerTable, customerId, carId, Definitions.RESERVATION_CAR);
    }

    /*
     * ==========================================================================
     * === manager_reserveRoom -- Returns failure if the room or customer does
     * not exist -- Returns TRUE on success, else FALSE
     * ==========================
     * ===================================================
     */
    public boolean manager_reserveRoom(CacheWrapper cache, int customerId, int roomId) {
	return reserve(cache, roomTable, customerTable, customerId, roomId, Definitions.RESERVATION_ROOM);
    }

    /*
     * ==========================================================================
     * === manager_reserveFlight -- Returns failure if the flight or customer
     * does not exist -- Returns TRUE on success, else FALSE
     * ====================
     * =========================================================
     */
    public boolean manager_reserveFlight(CacheWrapper cache, int customerId, int flightId) {
	return reserve(cache, flightTable, customerTable, customerId, flightId, Definitions.RESERVATION_FLIGHT);
    }

    /*
     * ==========================================================================
     * === cancel -- Customer is not allowed to cancel multiple times -- Returns
     * TRUE on success, else FALSE
     * ==============================================
     * ===============================
     */
    static boolean cancel(CacheWrapper cache, RBTree<Integer, Reservation> table, RBTree<Integer, Customer> customerTable, int customerId, int id, int type) {
	Customer customer;
	Reservation reservation;

	customer = customerTable.get(cache, customerId);
	if (customer == null) {
	    return false;
	}

	reservation = table.get(cache, id);
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
	return cancel(cache, carTable, customerTable, customerId, carId, Definitions.RESERVATION_CAR);
    }

    /*
     * ==========================================================================
     * === manager_cancelRoom -- Returns failure if the room, reservation, or
     * customer does not exist -- Returns TRUE on success, else FALSE
     * ============
     * =================================================================
     */
    boolean manager_cancelRoom(CacheWrapper cache, int customerId, int roomId) {
	return cancel(cache, roomTable, customerTable, customerId, roomId, Definitions.RESERVATION_ROOM);
    }

    /*
     * ==========================================================================
     * === manager_cancelFlight -- Returns failure if the flight, reservation,
     * or customer does not exist -- Returns TRUE on success, else FALSE
     * ========
     * =====================================================================
     */
    boolean manager_cancelFlight(CacheWrapper cache, int customerId, int flightId) {
	return cancel(cache, flightTable, customerTable, customerId, flightId, Definitions.RESERVATION_FLIGHT);
    }
    
    public void manager_doCustomer(CacheWrapper wrapper) {
	LocatedKey key = wrapper.createKey("local" + Vacation.NODE_TARGET.get() + "-" + ((Vacation.NODE_TARGET.get() * 10000) + (VacationStressor.MY_NODE * 100) + VacationStressor.THREADID.get()), Vacation.NODE_TARGET.get());
	Vacation.put(wrapper, key, 1);
    }
}
