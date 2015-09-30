package org.radargun.tpcw.transaction;

import org.radargun.CacheWrapper;
import org.radargun.tpcw.domain.Book;

public class Interactions {

    public static void adminResponse(CacheWrapper wrapper, String uuid, String i_new_image, String i_new_thumbnail,
            String i_new_coststr, double i_new_costdbl, String c_id, String shopping_id) throws Throwable {
        
//        Book book = GetBook.getBook(wrapper, uuid);
        // TPCW_Database.adminUpdate(I_ID, I_NEW_COSTdbl.doubleValue(), I_NEW_IMAGE,I_NEW_THUMBNAIL);
    }
    
}
