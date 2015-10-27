package org.radargun.tpcc.domain;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.DomainObject;

import com.basho.riak.protobuf.AntidotePB.FpbValue;
import com.basho.riak.protobuf.AntidotePB.TpccItem;
import com.basho.riak.protobuf.AntidotePB.TpccNewOrder;
import com.google.protobuf.ByteString;

import java.io.Serializable;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public class NewOrder implements Serializable, DomainObject {

   private long no_o_id;

   private long no_d_id;

   private long no_w_id;

   public NewOrder() {

   }

   public NewOrder(long no_o_id, long no_d_id, long no_w_id) {
      this.no_o_id = no_o_id;
      this.no_d_id = no_d_id;
      this.no_w_id = no_w_id;
   }


   public long getNo_o_id() {
      return no_o_id;
   }

   public long getNo_d_id() {
      return no_d_id;
   }

   public long getNo_w_id() {
      return no_w_id;
   }

   public void setNo_o_id(long no_o_id) {
      this.no_o_id = no_o_id;
   }

   public void setNo_d_id(long no_d_id) {
      this.no_d_id = no_d_id;
   }

   public void setNo_w_id(long no_w_id) {
      this.no_w_id = no_w_id;
   }

   private String getKey() {

      return "NEWORDER_" + this.no_w_id + "_" + this.no_d_id + "_" + this.no_o_id;
   }

   @Override
   public void store(CacheWrapper wrapper, int nodeIndex) throws Throwable {
	   /*TpccNewOrder neworder = TpccNewOrder.newBuilder()
			   .setNoOId(no_o_id).setNoDId(no_d_id).setNoWId(no_w_id)
			   .build();
	   
	   FpbValue value = FpbValue.newBuilder().setNeworder(neworder).setField(7).build();
	   */
	   
	   FpbValue value = FpbValue.newBuilder()
			   .addLongValue(no_o_id).addLongValue(no_d_id)
			   .addLongValue(no_w_id).setField(7).build();
	   
       wrapper.put(null, wrapper.createKey(this.getKey(), nodeIndex), value);
   }

   @Override
   public void storeToPopulate(CacheWrapper wrapper, int nodeIndex, boolean localOnly) throws Throwable {
      if (localOnly) {
         wrapper.putIfLocal(null, getKey(), this);
      } else {
         store(wrapper, nodeIndex);
      }
   }

   @Override
   public boolean load(CacheWrapper wrapper, int nodeIndex) throws Throwable {
	   
	  FpbValue value = (FpbValue)wrapper.get(null, wrapper.createKey(this.getKey(), nodeIndex));
	  if (value == null) return false;
	  
	  this.no_o_id = value.getLongValue(0);
	  this.no_d_id = value.getLongValue(1);
	  this.no_w_id = value.getLongValue(2);
	  
      return true;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NewOrder newOrder = (NewOrder) o;

      if (no_d_id != newOrder.no_d_id) return false;
      if (no_o_id != newOrder.no_o_id) return false;
      if (no_w_id != newOrder.no_w_id) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (int) (no_o_id ^ (no_o_id >>> 32));
      result = 31 * result + (int) (no_d_id ^ (no_d_id >>> 32));
      result = 31 * result + (int) (no_w_id ^ (no_w_id >>> 32));
      return result;
   }

}
