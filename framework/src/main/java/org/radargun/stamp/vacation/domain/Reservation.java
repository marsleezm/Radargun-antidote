package org.radargun.stamp.vacation.domain;

/* =============================================================================
 *
 * reservation.c
 * -- Representation of car, flight, and hotel relations
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

import java.io.Serializable;
import java.util.UUID;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.stamp.vacation.OpacityException;
import org.radargun.stamp.vacation.Vacation;

public class Reservation implements Comparable<Reservation>, Serializable {
    /* final */ int id;
    int node;
    /* final */ String PREFIX; 
    static final String NUM_USED = "numUsed";
    static final String NUM_FREE = "numFree";
    static final String NUM_TOTAL = "numTotal";
    static final String PRICE = "price";

    public Reservation() { }
    
    public Reservation(CacheWrapper cache, String type, int id, int numTotal, int price) {
	this.node = Vacation.NODE_TARGET.get();
	this.id = id;
	this.PREFIX = UUID.randomUUID().toString() + ":" + "Reservation:" + type + ":" + id + ":";
	LocatedKey key = cache.createKey( PREFIX + NUM_USED + node, node);
	Vacation.put(cache, key, 0);
	key = cache.createKey( PREFIX + NUM_FREE + node, node);
	Vacation.put(cache, key, numTotal);
	key = cache.createKey( PREFIX + NUM_TOTAL + node, node);
	Vacation.put(cache, key, numTotal);
	key = cache.createKey( PREFIX + PRICE + node, node);
	Vacation.put(cache, key, price);
	checkReservation(cache);
    }
    
    public Integer getNumUsed(CacheWrapper cache) {
	LocatedKey key = cache.createKey( PREFIX + NUM_USED + node, node);
	return (Integer) Vacation.get(cache, key);
    }
    
    public Integer getNumFree(CacheWrapper cache) {
	LocatedKey key = cache.createKey( PREFIX + NUM_FREE + node, node);
	return (Integer) Vacation.get(cache, key);
    }
    
    public Integer getNumTotal(CacheWrapper cache) {
	LocatedKey key = cache.createKey( PREFIX + NUM_TOTAL + node, node);
	return (Integer) Vacation.get(cache, key);
    }
    
    public Integer getPrice(CacheWrapper cache) {
	LocatedKey key = cache.createKey( PREFIX + PRICE + node, node);
	return (Integer) Vacation.get(cache, key);
    }
    
    public void putNumUsed(CacheWrapper cache, Integer value) {
	LocatedKey key = cache.createKey( PREFIX + NUM_USED + node, node);
	Vacation.put(cache, key, value);
    }
    
    public void putNumFree(CacheWrapper cache, Integer value) {
	LocatedKey key = cache.createKey( PREFIX + NUM_FREE + node, node);
	Vacation.put(cache, key, value);
    }
    
    public void putNumTotal(CacheWrapper cache, Integer value) {
	LocatedKey key = cache.createKey( PREFIX + NUM_TOTAL + node, node);
	Vacation.put(cache, key, value);
    }
    
    public void putPrice(CacheWrapper cache, Integer value) {
	LocatedKey key = cache.createKey( PREFIX + PRICE + node, node);
	Vacation.put(cache, key, value);
    }

    public void checkReservation(CacheWrapper cache) {
	int numUsed = this.getNumUsed(cache);
	if (numUsed < 0) {
	    throw new OpacityException();
	}

	int numFree = this.getNumFree(cache);
	if (numFree < 0) {
	    throw new OpacityException();
	}

	int numTotal = this.getNumTotal(cache);
	if (numTotal < 0) {
	    throw new OpacityException();
	}

	if ((numUsed + numFree) != numTotal) {
	    throw new OpacityException();
	}

	int price = this.getPrice(cache);
	if (price < 0) {
	    throw new OpacityException();
	} 
    }

    boolean reservation_addToTotal(CacheWrapper cache, int num) {
	if (getNumFree(cache) + num < 0) {
	    return false;
	}

	putNumFree(cache, getNumFree(cache) + num);
	putNumTotal(cache, getNumTotal(cache) + num);
	checkReservation(cache);
	return true;
    }

    /*
     * ==========================================================================
     * === reservation_make -- Returns TRUE on success, else FALSE
     * ==============
     * ===============================================================
     */
    public boolean reservation_make(CacheWrapper cache) {
	if (getNumFree(cache) < 1) {
	    return false;
	}
	putNumUsed(cache, getNumUsed(cache) + 1);
	putNumFree(cache, getNumFree(cache) - 1);
	checkReservation(cache);
	return true;
    }

    /*
     * ==========================================================================
     * === reservation_cancel -- Returns TRUE on success, else FALSE
     * ============
     * =================================================================
     */
    boolean reservation_cancel(CacheWrapper cache) {
	if (getNumUsed(cache) < 1) {
	    return false;
	}
	putNumUsed(cache, getNumUsed(cache) - 1);
	putNumFree(cache, getNumFree(cache) + 1);
	checkReservation(cache);
	return true;
    }

    /*
     * ==========================================================================
     * === reservation_updatePrice -- Failure if 'price' < 0 -- Returns TRUE on
     * success, else FALSE
     * ======================================================
     * =======================
     */
    boolean reservation_updatePrice(CacheWrapper cache, int newPrice) {
	if (newPrice < 0) {
	    return false;
	}

	putPrice(cache, newPrice);
	checkReservation(cache);
	return true;
    }

    /*
     * ==========================================================================
     * === reservation_compare -- Returns -1 if A < B, 0 if A = B, 1 if A > B
     * ====
     * =========================================================================
     */
    int reservation_compare(Reservation aPtr, Reservation bPtr) {
	return aPtr.id - bPtr.id;
    }

    /*
     * ==========================================================================
     * === reservation_hash
     * ======================================================
     * =======================
     */
    int reservation_hash() {
	return id;
    }

    @Override
    public int compareTo(Reservation arg0) {
	int myId = this.id;
	int hisId = arg0.id;
	if (myId < hisId) {
	    return -1;
	} else if (myId == hisId) {
	    return 0;
	} else {
	    return 1;
	}
    }

}
