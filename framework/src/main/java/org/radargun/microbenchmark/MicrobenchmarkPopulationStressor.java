package org.radargun.microbenchmark;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stressors.AbstractCacheWrapperStressor;

public class MicrobenchmarkPopulationStressor extends AbstractCacheWrapperStressor {

    private static Log log = LogFactory.getLog(MicrobenchmarkPopulationStressor.class);

    private int items;
    private int range;
    private String set;
    private int clients;

    public void setItems(int items) {
        this.items = items;
    }

    public void setRange(int range) {
        this.range = range;
    }

    public void setSet(String set) {
        this.set = set;
    }
    
    public void setClients(int clients) {
        this.clients = clients;
    }

    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
        if (wrapper == null) {
            throw new IllegalStateException("Null wrapper not allowed");
        }
        try {
            log.info("Performing Population Operations");
            new MicrobenchmarkPopulation(wrapper, items, range, set, clients).performPopulation();
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("Received exception during cache population" + e.getMessage());
        }
        return null;
    }

    @Override
    public void destroy() throws Exception {
        //Don't destroy data in cache!
    }

}
