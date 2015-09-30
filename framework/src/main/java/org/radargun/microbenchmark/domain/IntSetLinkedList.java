package org.radargun.microbenchmark.domain;

import java.io.Serializable;
import java.util.UUID;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.MicrobenchmarkStressor;


public class IntSetLinkedList implements IntSet, Serializable {

    public class Node implements Serializable {
        /* final */ private int m_value;
        private String uuid;

        public Node(CacheWrapper wrapper, int value, Node next) {
            m_value = value;
            this.uuid = UUID.randomUUID().toString();
            setNext(wrapper, next);
        }

        public Node(CacheWrapper wrapper, int value) {
            this(wrapper, value, null);
        }

        public int getValue() {
            return m_value;
        }

        public void setNext(CacheWrapper wrapper, Node next) {
            if (next != null) {
                LocatedKey key = wrapper.createKey(m_value + ":" + uuid + ":next", node);
                Micro.put(wrapper, key, next);
            }
        }

        public Node getNext(CacheWrapper wrapper) {
            LocatedKey key = wrapper.createKey(m_value + ":" + uuid + ":next", node);
            return (Node) Micro.get(wrapper, key);
        }
    }

    /* final */ private Node m_first;

    int node;
    
    public IntSetLinkedList() { }

    public IntSetLinkedList(int node, CacheWrapper wrapper) {
        this.node = node;
        Node min = new Node(wrapper, Integer.MIN_VALUE);
        Node max = new Node(wrapper, Integer.MAX_VALUE);
        min.setNext(wrapper, max);
        m_first = min;
    }

    public boolean add(CacheWrapper wrapper, int value, boolean local, boolean remote) {
        boolean result;

        Node previous = m_first;
        Node next = previous.getNext(wrapper);
        int v;
        while ((v = next.getValue()) < value) {
            previous = next;
            next = previous.getNext(wrapper);
        }
        result = v != value;
        if (result && !remote) {
            previous.setNext(wrapper, new Node(wrapper, value, next));
        } else {
            LocatedKey key = wrapper.createKey("local" + (((wrapper.getMyNode() + 1) * 1000) + node) + "-" + MicrobenchmarkStressor.THREADID.get(), node);
            Micro.put(wrapper, key, 1);
        }

        return result;
    }

    public boolean remove(CacheWrapper wrapper, final int value, boolean local, boolean remote) {
        boolean result;

        Node previous = m_first;
        Node next = previous.getNext(wrapper);
        int v;
        while ((v = next.getValue()) < value) {
            previous = next;
            next = previous.getNext(wrapper);
        }
        result = v == value;
        if (result && !remote) {
            previous.setNext(wrapper, next.getNext(wrapper));
        } else {
            LocatedKey key = wrapper.createKey("local" + (((wrapper.getMyNode() + 1) * 1000) + node) + "-" + MicrobenchmarkStressor.THREADID.get(), node);
            Micro.put(wrapper, key, 1);
        }

        return result;
    }

    public boolean contains(CacheWrapper wrapper, final int value) {
        boolean result;

        Node previous = m_first;
        Node next = previous.getNext(wrapper);
        int v;
        while ((v = next.getValue()) < value) {
            previous = next;
            next = previous.getNext(wrapper);
        }
        result = (v == value);

        return result;
    }
}
