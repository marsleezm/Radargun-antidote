package org.radargun.microbenchmark.domain;

import java.io.Serializable;
import java.util.Random;
import java.util.UUID;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.MicrobenchmarkStressor;

public class IntSetSkipList implements IntSet, Serializable {

    public class Node implements Serializable {
        /* final */ private int m_value;
        /* final */ private int level;
        /* final */ private String uuid;

        public Node(int level, int value) {
            this.level = level;
            this.m_value = value;
            this.uuid = UUID.randomUUID().toString();
        }

        public void setForward(CacheWrapper cache, int index, Node forward) {
            LocatedKey key = cache.createKey(uuid + ":" + m_value + ":" + index + ":next", node);
            Micro.put(cache, key, forward);
        }

        public Node getForward(CacheWrapper cache, int index) {
            LocatedKey key = cache.createKey(uuid + ":" + m_value + ":" + index + ":next", node);
            return (Node) Micro.get(cache, key);
        }

        public int getValue() {
            return m_value;
        }

        public int getLevel() {
            return level;
        }

    }

    // Probability to increase level
    /* final */ private double m_probability = 0.25;

    // Upper bound on the number of levels
    /* final */ private int m_maxLevel = 32;

    // Highest level so far: level in cache

    // First element of the list
    /* final */ private Node m_head;
    // Thread-private PRNG
    /* final */ private static ThreadLocal<Random> s_random = new ThreadLocal<Random>() {
        protected synchronized Random initialValue() {
            return new Random();
        }
    };

    private void setLevel(CacheWrapper cache, int level) {
        LocatedKey key = cache.createKey(node + "skipList:level", node);
        Micro.put(cache, key, level);
    }

    private Integer getLevel(CacheWrapper cache) {
        LocatedKey key = cache.createKey(node + "skipList:level", node);
        return (Integer) Micro.get(cache, key);
    }

    public IntSetSkipList() { }

    private int node;
    
    public IntSetSkipList(int node, CacheWrapper cache) {
        this.node = node;
        setLevel(cache, 0);

        m_head = new Node(m_maxLevel, Integer.MIN_VALUE);
        Node tail = new Node(m_maxLevel, Integer.MAX_VALUE);
        for (int i = 0; i <= m_maxLevel; i++)
            m_head.setForward(cache, i, tail);
    }

    protected int randomLevel() {
        int l = 0;
        while (l < m_maxLevel && s_random.get().nextDouble() < m_probability)
            l++;
        return l;
    }

    public boolean add(CacheWrapper cache, int value, boolean local, boolean remote) {
        boolean result;

        Node[] update = new Node[m_maxLevel + 1];
        Node node = m_head;
        int level = getLevel(cache);

        for (int i = level; i >= 0; i--) {
            Node next = node.getForward(cache, i);
            while (next.getValue() < value) {
                node = next;
                next = node.getForward(cache, i);
            }
            update[i] = node;
        }
        node = node.getForward(cache, 0);

        if (node.getValue() != value && !remote) {
            int newLevel = randomLevel();
            if (newLevel > level) {
                for (int i = level + 1; i <= level; i++)
                    update[i] = m_head;
                setLevel(cache, level);
            }
            node = new Node(level, value);
            for (int i = 0; i <= level; i++) {
                node.setForward(cache, i, update[i].getForward(cache, i));
                update[i].setForward(cache, i, node);
            }
            result = true;
        } else {
            LocatedKey key = cache.createKey("local" + (((cache.getMyNode() + 1) * 1000) + this.node) + "-" + MicrobenchmarkStressor.THREADID.get(), this.node);
            Micro.put(cache, key, 1);
            return false;
        }


        return result;
    }

    public boolean remove(CacheWrapper wrapper, int value, boolean local, boolean remote) {
        boolean result;

        Node[] update = new Node[m_maxLevel + 1];
        Node node = m_head;

        int level = getLevel(wrapper);

        for (int i = level; i >= 0; i--) {
            Node next = node.getForward(wrapper, i);
            while (next.getValue() < value) {
                node = next;
                next = node.getForward(wrapper, i);
            }
            update[i] = node;
        }
        node = node.getForward(wrapper, 0);

        if (node.getValue() == value && !remote) {
            for (int i = 0; i <= level; i++) {
                if (update[i].getForward(wrapper, i).getValue() == node.getValue())
                    update[i].setForward(wrapper, i, node.getForward(wrapper, i));
            }

            while (level > 0 && m_head.getForward(wrapper, level).getForward(wrapper, 0) == null) {
                level--;
                setLevel(wrapper, level);
            }           
            result = true;
        } else {
            LocatedKey key = wrapper.createKey("local" + (((wrapper.getMyNode() + 1) * 1000) + this.node) + "-" + MicrobenchmarkStressor.THREADID.get(), this.node);
            Micro.put(wrapper, key, 1);
            return false;
        }

        return result;
    }

    public boolean contains(CacheWrapper cache, int value) {
        boolean result;

        Node node = m_head;
        int initialM_Level = getLevel(cache);

        for (int i = initialM_Level; i >= 0; i--) {
            Node next = node.getForward(cache, i);
            while (next.getValue() < value) {
                node = next;
                next = node.getForward(cache, i);
            }
        }
        node = node.getForward(cache, 0);

        result = (node.getValue() == value);

        return result;

    }

}
