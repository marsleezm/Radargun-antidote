package org.radargun.tpcw.transaction;

import org.radargun.CacheWrapper;
import org.radargun.tpcw.domain.Book;

public class GetBook implements TPCWPartialInteraction<Book> {

    private final CacheWrapper wrapper;
    private final String uuid;

    public GetBook(CacheWrapper wrapper, String uuid) {
        this.wrapper = wrapper;
        this.uuid = uuid;
    }
    
    @Override
    public Book interact() throws Throwable {
        Book lookup = new Book();
        lookup.setUUID(uuid);
        lookup.load(wrapper);
        return lookup;        
    }
    
}
