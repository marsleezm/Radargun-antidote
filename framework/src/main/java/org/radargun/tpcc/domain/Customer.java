package org.radargun.tpcc.domain;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.tpcc.DomainObject;
import org.radargun.tpcc.TpccTools;

import com.basho.riak.protobuf.AntidotePB.FpbValue;
import com.basho.riak.protobuf.AntidotePB.TpccCustomer;
import com.basho.riak.protobuf.AntidotePB.TpccDistrict;
import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.Date;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public class Customer implements Serializable, Comparable, DomainObject {

   /* district id */
   private long c_d_id;

   /* warehouse id */
   private long c_w_id;

   private long c_id;

   private String c_first;

   private String c_middle;

   private String c_last;

   private String c_street1;

   private String c_street2;

   private String c_city;

   private String c_state;

   private String c_zip;

   private String c_phone;

   private long c_since;

   private String c_credit;

   private double c_credit_lim;

   private double c_discount;

   // private double c_balance;

   private double c_ytd_payment;

   private int c_payment_cnt;

   private int c_delivery_cnt;

   private String c_data;

   public Customer() {

   }

   public Customer(CacheWrapper wrapper, int nodeIndex, long c_w_id, long c_d_id, long c_id, String c_first, String c_middle, String c_last, String c_street1, String c_street2, String c_city, String c_state, String c_zip, String c_phone, Date c_since, String c_credit, double c_credit_lim, double c_discount, double c_balance, double c_ytd_payment, int c_payment_cnt, int c_delivery_cnt, String c_data) {
      this.c_w_id = c_w_id;
      this.c_d_id = c_d_id;
      this.c_id = c_id;
      this.c_first = c_first;
      this.c_middle = c_middle;
      this.c_last = c_last;
      this.c_street1 = c_street1;
      this.c_street2 = c_street2;
      this.c_city = c_city;
      this.c_state = c_state;
      this.c_zip = c_zip;
      this.c_phone = c_phone;
      this.c_since = (c_since == null) ? -1 : c_since.getTime();
      this.c_credit = c_credit;
      this.c_credit_lim = c_credit_lim;
      this.c_discount = c_discount;
      setC_balance(wrapper, nodeIndex, c_balance);
      this.c_ytd_payment = c_ytd_payment;
      this.c_payment_cnt = c_payment_cnt;
      this.c_delivery_cnt = c_delivery_cnt;
      this.c_data = c_data;
   }

   public long getC_w_id() {
      return c_w_id;
   }

   public long getC_d_id() {
      return c_d_id;
   }

   public long getC_id() {
      return c_id;
   }

   public String getC_first() {
      return c_first;
   }

   public String getC_middle() {
      return c_middle;
   }

   public String getC_last() {
      return c_last;
   }

   public String getC_street1() {
      return c_street1;
   }

   public String getC_street2() {
      return c_street2;
   }

   public String getC_city() {
      return c_city;
   }

   public String getC_state() {
      return c_state;
   }

   public String getC_zip() {
      return c_zip;
   }

   public String getC_phone() {
      return c_phone;
   }

   public Date getC_since() {
      return (c_since == -1) ? null : new Date(c_since);
   }

   public String getC_credit() {
      return c_credit;
   }

   public double getC_credit_lim() {
      return c_credit_lim;
   }

   public double getC_discount() {
      return c_discount;
   }

   public Double getC_balance(CacheWrapper wrapper, int nodeIndex) {
       return TpccTools.get(wrapper, wrapper.createKey(getKeyC_balance(), nodeIndex));
   }

   public String getKeyC_balance() {
       return this.getKey() + ":c_balance";
   }
   
   public double getC_ytd_payment() {
      return c_ytd_payment;
   }

   public int getC_payment_cnt() {
      return c_payment_cnt;
   }

   public int getC_delivery_cnt() {
      return c_delivery_cnt;
   }

   public String getC_data() {
      return c_data;
   }

   public void setC_w_id(long c_w_id) {
      this.c_w_id = c_w_id;
   }

   public void setC_d_id(long c_d_id) {
      this.c_d_id = c_d_id;
   }

   public void setC_id(long c_id) {
      this.c_id = c_id;
   }

   public void setC_first(String c_first) {
      this.c_first = c_first;
   }

   public void setC_middle(String c_middle) {
      this.c_middle = c_middle;
   }

   public void setC_last(String c_last) {
      this.c_last = c_last;
   }

   public void setC_street1(String c_street1) {
      this.c_street1 = c_street1;
   }

   public void setC_street2(String c_street2) {
      this.c_street2 = c_street2;
   }

   public void setC_city(String c_city) {
      this.c_city = c_city;
   }

   public void setC_state(String c_state) {
      this.c_state = c_state;
   }

   public void setC_zip(String c_zip) {
      this.c_zip = c_zip;
   }

   public void setC_phone(String c_phone) {
      this.c_phone = c_phone;
   }

   public void setC_since(Date c_since) {
      this.c_since = (c_since == null) ? -1 : c_since.getTime();
   }

   public void setC_credit(String c_credit) {
      this.c_credit = c_credit;
   }

   public void setC_credit_lim(double c_credit_lim) {
      this.c_credit_lim = c_credit_lim;
   }

   public void setC_discount(double c_discount) {
      this.c_discount = c_discount;
   }

   public void setC_balance(CacheWrapper wrapper, int nodeIndex, double c_balance) {
       TpccTools.put(wrapper, wrapper.createKey(getKeyC_balance(), nodeIndex), c_balance);
   }
   
   public void setC_ytd_payment(double c_ytd_payment) {
      this.c_ytd_payment = c_ytd_payment;
   }

   public void setC_payment_cnt(int c_payment_cnt) {
      this.c_payment_cnt = c_payment_cnt;
   }

   public void setC_delivery_cnt(int c_delivery_cnt) {
      this.c_delivery_cnt = c_delivery_cnt;
   }

   public void setC_data(String c_data) {
      this.c_data = c_data;
   }

   private String getKey() {
      return "CUSTOMER_" + this.c_w_id + "_" + this.c_d_id + "_" + this.c_id;
   }

   @Override
   public void store(CacheWrapper wrapper, int nodeIndex) throws Throwable {
	   TpccCustomer customer = TpccCustomer.newBuilder()
			   .setCCity(c_city).setCCredit(c_credit)
			   .setCCreditLim(c_credit_lim).setCData((c_data))
			   .setCDeliveryCnt(c_delivery_cnt)
			   .setCDiscount(c_discount).setCFirst((c_first))
			   .setCLast((c_last)).setCMiddle((c_middle))
			   .setCPaymentCnt(c_payment_cnt).setCPhone((c_phone))
			   .setCSince(c_since).setCState((c_state))
			   .setCStreet1((c_street1)).setCStreet2((c_street2))
			   .setCYtdPayment(c_ytd_payment).setCZip((c_zip)).build();
	   
	   FpbValue value = FpbValue.newBuilder().setCustomer(customer).setField(2).build();
	      
       wrapper.put(null, wrapper.createKey(this.getKey(), nodeIndex) , value);
   }

   @Override
   public void storeToPopulate(CacheWrapper wrapper, int nodeIndex, boolean localOnly) throws Throwable {
      String key = getKey();
      if (localOnly) {
    	  
      } else {
         store(wrapper, nodeIndex);
      }
   }

   @Override
   public boolean load(CacheWrapper wrapper, int nodeIndex) throws Throwable {

      FpbValue value = (FpbValue)wrapper.get(null, wrapper.createKey(this.getKey(), nodeIndex));
      if (value == null) return false;
      
      TpccCustomer customer = value.getCustomer();

      this.c_city = customer.getCCity();
      this.c_credit = customer.getCCredit();
      this.c_credit_lim = customer.getCCreditLim();
      this.c_data = customer.getCData();
      this.c_delivery_cnt = customer.getCDeliveryCnt();
      this.c_discount = customer.getCDiscount();
      this.c_first = customer.getCFirst();
      this.c_last = customer.getCLast();
      this.c_middle = customer.getCMiddle();
      this.c_payment_cnt = customer.getCPaymentCnt();
      this.c_phone = customer.getCPhone();
      this.c_since = customer.getCSince();
      this.c_state = customer.getCState();
      this.c_street1 = customer.getCStreet1();
      this.c_street2 = customer.getCStreet2();
      this.c_ytd_payment = customer.getCYtdPayment();
      this.c_zip = customer.getCZip();

      return true;
   }

   @Override
   public int compareTo(Object o) {
      if (o == null || !(o instanceof Customer)) return -1;

      Customer other = (Customer) o;
      if (this.c_first != null)
         return this.c_first.compareTo(other.c_first);
      else if (other.c_first != null)
         return 1;
      else
         return 0;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Customer customer = (Customer) o;

      if (Double.compare(customer.c_credit_lim, c_credit_lim) != 0) return false;
      if (c_d_id != customer.c_d_id) return false;
      if (c_delivery_cnt != customer.c_delivery_cnt) return false;
      if (Double.compare(customer.c_discount, c_discount) != 0) return false;
      if (c_id != customer.c_id) return false;
      if (c_payment_cnt != customer.c_payment_cnt) return false;
      if (c_since != customer.c_since) return false;
      if (c_w_id != customer.c_w_id) return false;
      if (Double.compare(customer.c_ytd_payment, c_ytd_payment) != 0) return false;
      if (c_city != null ? !c_city.equals(customer.c_city) : customer.c_city != null) return false;
      if (c_credit != null ? !c_credit.equals(customer.c_credit) : customer.c_credit != null) return false;
      if (c_data != null ? !c_data.equals(customer.c_data) : customer.c_data != null) return false;
      if (c_first != null ? !c_first.equals(customer.c_first) : customer.c_first != null) return false;
      if (c_last != null ? !c_last.equals(customer.c_last) : customer.c_last != null) return false;
      if (c_middle != null ? !c_middle.equals(customer.c_middle) : customer.c_middle != null) return false;
      if (c_phone != null ? !c_phone.equals(customer.c_phone) : customer.c_phone != null) return false;
      if (c_state != null ? !c_state.equals(customer.c_state) : customer.c_state != null) return false;
      if (c_street1 != null ? !c_street1.equals(customer.c_street1) : customer.c_street1 != null) return false;
      if (c_street2 != null ? !c_street2.equals(customer.c_street2) : customer.c_street2 != null) return false;
      if (c_zip != null ? !c_zip.equals(customer.c_zip) : customer.c_zip != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result;
      long temp;
      result = (int) (c_w_id ^ (c_w_id >>> 32));
      result = 31 * result + (int) (c_d_id ^ (c_d_id >>> 32));
      result = 31 * result + (int) (c_id ^ (c_id >>> 32));
      result = 31 * result + (c_first != null ? c_first.hashCode() : 0);
      result = 31 * result + (c_middle != null ? c_middle.hashCode() : 0);
      result = 31 * result + (c_last != null ? c_last.hashCode() : 0);
      result = 31 * result + (c_street1 != null ? c_street1.hashCode() : 0);
      result = 31 * result + (c_street2 != null ? c_street2.hashCode() : 0);
      result = 31 * result + (c_city != null ? c_city.hashCode() : 0);
      result = 31 * result + (c_state != null ? c_state.hashCode() : 0);
      result = 31 * result + (c_zip != null ? c_zip.hashCode() : 0);
      result = 31 * result + (c_phone != null ? c_phone.hashCode() : 0);
      result = 31 * result + (int) (c_since ^ (c_since >>> 32));
      result = 31 * result + (c_credit != null ? c_credit.hashCode() : 0);
      temp = c_credit_lim != +0.0d ? Double.doubleToLongBits(c_credit_lim) : 0L;
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      temp = c_discount != +0.0d ? Double.doubleToLongBits(c_discount) : 0L;
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      temp = c_ytd_payment != +0.0d ? Double.doubleToLongBits(c_ytd_payment) : 0L;
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      result = 31 * result + c_payment_cnt;
      result = 31 * result + c_delivery_cnt;
      result = 31 * result + (c_data != null ? c_data.hashCode() : 0);
      return result;

   }

}
