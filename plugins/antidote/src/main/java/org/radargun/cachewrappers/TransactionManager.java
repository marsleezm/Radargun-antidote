package org.radargun.cachewrappers;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.basho.riak.protobuf.AntidotePB.FpbPrepTxnReq;
import com.basho.riak.protobuf.AntidotePB.FpbPrepTxnResp;
import com.basho.riak.protobuf.AntidotePB.FpbReadReq;
import com.basho.riak.protobuf.AntidotePB.FpbStartTxnReq;
import com.basho.riak.protobuf.AntidotePB.FpbTxId;
import com.basho.riak.protobuf.AntidotePB.FpbUpdate;
import com.basho.riak.protobuf.AntidotePB.FpbValue;
import com.google.protobuf.ByteString;

import org.radargun.cachewrappers.AntidoteConnection;

public class TransactionManager {
    private AntidoteConnection connection = null;
    private static final Integer MSG_StartTxnReq = 90;
    private static final Integer MSG_PrepTxnReq = 91;
    private static final Integer MSG_PrepTxnResp = 92;
    private static final Integer MSG_ReadReq = 93;
    private static final Integer MSG_TxId = 95;
    private static final Integer MSG_Value = 99;
    
    
    private FpbTxId txId;
    private Map<Integer, String> writeBuffer = new HashMap<Integer, String>();
    private Map<Integer, String> readBuffer = new HashMap<Integer, String>();
	
	public void begin() throws IOException {
		
		FpbStartTxnReq startTxnReq = FpbStartTxnReq.newBuilder().setClock(0).build();
		connection.send(MSG_StartTxnReq, startTxnReq);
		
		txId = FpbTxId.parseFrom(connection.receive(MSG_TxId));
	}
	
	public void put(Object key, Object value) {
		writeBuffer.put((Integer)key, (String)value);
	}

	public Object get(Object key) {
		String value = writeBuffer.get(key);
		if( value == null)
		{
			value = readBuffer.get(key);
			if ( value == null)
			{
				String server = DCManager.getNodeByKey((Integer)key);
				value = getKeyFromServer(server, (Integer)key);
				readBuffer.put((Integer)key, value);
				return value;
			}
			else
				return value;
		}
		else
		{
			return value;
		}
	}

	public void commit() throws Exception {
		FpbPrepTxnReq.Builder prepReqBuilder = FpbPrepTxnReq.newBuilder().setTxid(txId);
		int index = 1;
		for(Map.Entry<Integer, String> entry : writeBuffer.entrySet())
		{
			prepReqBuilder.addOps(index++, 
					FpbUpdate.newBuilder().setKey(entry.getKey()).setValue(
							ByteString.copyFromUtf8((entry.getValue()))));
		}
		connection = DCManager.getLocalConnection();
		connection.send(MSG_PrepTxnReq, prepReqBuilder.build());
		FpbPrepTxnResp resp = FpbPrepTxnResp.parseFrom(connection.receive(MSG_PrepTxnResp));
		if (!resp.getSuccess())
			throw new Exception();
	}

	public void abort() {
		// TODO Auto-generated method stub
		System.out.println("Something is wrong!!!");
	}

	public Object getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	public void delayedComputation() {
		// TODO Auto-generated method stub
		
	}

	public Object delayedGet(Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	public void delayedPut(Object key, Object value) {
		// TODO Auto-generated method stub
		
	}

	public boolean isCoordinator() {
		// TODO Auto-generated method stub
		return true;
	}
	
	private String getKeyFromServer(String server, Integer key) {
		AntidoteConnection connection = DCManager.getConnection(server);
		FpbReadReq readReq = FpbReadReq.newBuilder().setTxid(txId).setKey(key).build();
		try {
			connection.send(MSG_ReadReq, readReq);
			FpbValue value = FpbValue.parseFrom(connection.receive(MSG_Value));
			return value.getValue().toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		
	}

}
