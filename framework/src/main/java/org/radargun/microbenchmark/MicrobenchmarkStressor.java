package org.radargun.microbenchmark;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.microbenchmark.transaction.AddTransaction;
import org.radargun.microbenchmark.transaction.ContainsTransaction;
import org.radargun.microbenchmark.transaction.MicrobenchmarkTransaction;
import org.radargun.microbenchmark.transaction.RemoveTransaction;
import org.radargun.stressors.AbstractCacheWrapperStressor;

public class MicrobenchmarkStressor extends AbstractCacheWrapperStressor implements Runnable {

    private static Log log = LogFactory.getLog(MicrobenchmarkStressor.class);

    public static final int TEST_PHASE = 2;
    public static final int SHUTDOWN_PHASE = 3;

    private CacheWrapper cacheWrapper;
    private long restarts = 0;
    private long steps = 0;

    private int range;
    public static int clients;
    private int writeRatio;
    private boolean totalOrder;

    private boolean m_write = true;
    private int m_last;
    private Random m_random = new Random();

    public static final ThreadLocal<Integer> THREADID = new ThreadLocal<Integer>() {};
    
    volatile protected int m_phase = TEST_PHASE;

    private int threadid;

    private int remote;

    public MicrobenchmarkStressor(int threadid) {
        this.threadid = threadid;
    }

    public void setCacheWrapper(CacheWrapper cacheWrapper) {
        this.cacheWrapper = cacheWrapper;
    }

    @Override
    public void run() {
        THREADID.set(this.threadid);
        stress(cacheWrapper);
    }

    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
        if (wrapper == null) {
            throw new IllegalStateException("Null wrapper not allowed");
        }

        this.cacheWrapper = wrapper;

        while (m_phase == TEST_PHASE) {
            step(TEST_PHASE);
            steps++;
        }

        Map<String, String> results = new LinkedHashMap<String, String>();

        return results;
    }

    protected void step(int phase) {
        int k = Math.abs(Math.abs(m_random.nextInt()) % 100);
        int node = -1;
	boolean remote = false;
        if (k < (this.remote * 2)) {
	    remote = true;
            node = Math.abs(Math.abs(m_random.nextInt()) % clients);
        } else {
            node = cacheWrapper.getMyNode();
        }
        int i = m_random.nextInt(100);
        boolean local = ((i * k) % 100) < 100;
        if (i < writeRatio) {
            if (m_write) {
                m_last = m_random.nextInt(range);
                if (processTransaction(cacheWrapper, new AddTransaction(node, m_last, local, remote, totalOrder)))
                    m_write = false;
            } else {
                processTransaction(cacheWrapper, new RemoveTransaction(cacheWrapper.getMyNode(), m_last, local, remote, totalOrder));
                m_write = true;
            }
        } else {
            node = cacheWrapper.getMyNode();
            processTransaction(cacheWrapper, new ContainsTransaction(node, m_last));
        }
    }

    private boolean processTransaction(CacheWrapper wrapper, MicrobenchmarkTransaction transaction) {
        boolean successful = true;
        boolean result = false;

        while (true) {
            if (m_phase == SHUTDOWN_PHASE) {
                return false;
            }
            result = false;
            cacheWrapper.startTransaction(transaction.isReadOnly());
            try {
                result = transaction.executeTransaction(cacheWrapper);
            } catch (Throwable e) {
        	e.printStackTrace();
                successful = false;
            }

            try {
                cacheWrapper.endTransaction(successful);

                if (!successful) {
                    setRestarts(getRestarts() + 1);
                }
            } catch (Throwable rb) {
		rb.printStackTrace();
                setRestarts(getRestarts() + 1);
                successful = false;
            }

            if (! successful) {
                successful = true;
            } else { 
                break;
            }
        }

        return result;
    }

    @Override
    public void destroy() throws Exception {
        cacheWrapper.empty();
        cacheWrapper = null;
    }

    public long getRestarts() {
        return restarts;
    }

    public void setRestarts(long restarts) {
        this.restarts = restarts;
    }

    public long getSteps() {
        return steps;
    }

    public void setSteps(long steps) {
        this.steps = steps;
    }

    public int getRange() {
        return range;
    }

    public void setRange(int range) {
        this.range = range;
    }

    public int getClients() {
        return this.clients;
    }
    
    public void setClients(int clients) {
        this.clients = clients;
    }

    public int getWriteRatio() {
        return writeRatio;
    }

    public void setWriteRatio(int writeRatio) {
        this.writeRatio = writeRatio;
    }

    public boolean isM_write() {
        return m_write;
    }

    public void setM_write(boolean m_write) {
        this.m_write = m_write;
    }

    public int getM_last() {
        return m_last;
    }

    public void setM_last(int m_last) {
        this.m_last = m_last;
    }

    public Random getM_random() {
        return m_random;
    }

    public void setM_random(Random m_random) {
        this.m_random = m_random;
    }

    public int getM_phase() {
        return m_phase;
    }

    public void setM_phase(int m_phase) {
        this.m_phase = m_phase;
    }

    public void setTotalOrder(boolean totalOrder) {
	this.totalOrder = totalOrder;
    }

    public void setRemote(int remote) {
        this.remote = remote;
    }


}
