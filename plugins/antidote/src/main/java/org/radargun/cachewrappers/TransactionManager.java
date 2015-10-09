package org.radargun.cachewrappers;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.basho.riak.protobuf.AntidotePB.FpbNodeUps;
import com.basho.riak.protobuf.AntidotePB.FpbPerNodeUp;
import com.basho.riak.protobuf.AntidotePB.FpbPrepTxnReq;
import com.basho.riak.protobuf.AntidotePB.FpbPrepTxnResp;
import com.basho.riak.protobuf.AntidotePB.FpbReadReq;
import com.basho.riak.protobuf.AntidotePB.FpbSingleUpReq;
import com.basho.riak.protobuf.AntidotePB.FpbStartTxnReq;
import com.basho.riak.protobuf.AntidotePB.FpbTxId;
import com.basho.riak.protobuf.AntidotePB.FpbUpdate;
import com.basho.riak.protobuf.AntidotePB.FpbValue;
import com.google.protobuf.ByteString;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.cachewrappers.AntidoteConnection;

public class TransactionManager {
    private AntidoteConnection connection = null;
    private static final Integer MSG_StartTxnReq = 90;
    private static final Integer MSG_PrepTxnReq = 91;
    private static final Integer MSG_PrepTxnResp = 92;
    private static final Integer MSG_ReadReq = 93;
    private static final Integer MSG_TxId = 95;
    private static final Integer MSG_Value = 99;
    private static final Integer MSG_SingleUpReq = 100;
    private static Log log = LogFactory.getLog(TransactionManager.class);
    private boolean isInTxn = false;
    
    private FpbTxId txId;
    private Map<Object, String> writeBuffer = new HashMap<Object, String>();
    private Map<Object, String> readBuffer = new HashMap<Object, String>();
	
	public void begin() throws IOException {
		FpbStartTxnReq startTxnReq = FpbStartTxnReq.newBuilder().setClock(0).build();
		connection = DCManager.getLocalConnection();
		connection.send(MSG_StartTxnReq, startTxnReq);
		txId = FpbTxId.parseFrom(connection.receive(MSG_TxId));
		isInTxn = true;
	}
	
	public void put(Object key, Object value) throws Exception {
		if (isInTxn)
			writeBuffer.put(key, (String)value);
		else{
			//Partition id has to be plus one because of the index in Erlang is different.
			if( key instanceof MagicKey)
			{
				MagicKey mKey = (MagicKey)key;
				AntidoteConnection connection = DCManager.getConectionByIndex(mKey.node);
				Integer partitionId = mKey.hashCode() % DCManager.getPartNum(mKey.node);
				FpbSingleUpReq singleUpReq = FpbSingleUpReq.newBuilder().setKey(ByteString.copyFromUtf8(mKey.key))
						.setValue(ByteString.copyFromUtf8(value.toString())).setPartitionId(partitionId+1).build();
				
				connection.send(MSG_SingleUpReq, singleUpReq);
				FpbPrepTxnResp resp = FpbPrepTxnResp.parseFrom(connection.receive(MSG_PrepTxnResp));
				if (!resp.getSuccess())
					throw new Exception();
			}
			else
			{	
				Pair<Integer, Integer> location = DCManager.locateForNormalKey(key);
				AntidoteConnection connection = DCManager.getConectionByIndex(location.fst);
				FpbSingleUpReq singleUpReq = FpbSingleUpReq.newBuilder().setKey(ByteString.copyFromUtf8(key.toString()))
						.setValue(ByteString.copyFromUtf8(value.toString())).setPartitionId(location.snd+1).build();
				
				connection.send(MSG_SingleUpReq, singleUpReq);
				log.info("Sent message");
				FpbPrepTxnResp resp = FpbPrepTxnResp.parseFrom(connection.receive(MSG_PrepTxnResp));
				if (!resp.getSuccess())
					throw new Exception();
			}
		}
	}

	public Object get(Object key) {
		String value = writeBuffer.get(key);
		if( value == null)
		{
			value = readBuffer.get(key);
			if ( value == null)
			{
				value = getKeyFromServer(key);
				readBuffer.put(key, value);
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
		int localNodeIndex = DCManager.getNodeIndex();
		String localIp = DCManager.getLocalNodeIp();
		Map<Integer, FpbPerNodeUp.Builder> localKeySet = 
				new HashMap<Integer, FpbPerNodeUp.Builder>();
		Map<Integer, Map<Integer, FpbPerNodeUp.Builder>> remoteKeySet = 
				new HashMap<Integer, Map<Integer, FpbPerNodeUp.Builder>>();
		FpbNodeUps.Builder localUpdates = FpbNodeUps.newBuilder(),
				remoteUpdates = FpbNodeUps.newBuilder();
		
		for(Map.Entry<Object, String> entry : writeBuffer.entrySet())
		{	
			MagicKey mKey = (MagicKey)entry.getKey();
			int keyNode, hashCode;
			String realKey;
			keyNode = mKey.node;
			realKey = mKey.key;
			hashCode = mKey.hashCode();
			
			if (keyNode == localNodeIndex)
			{
				int index = hashCode % DCManager.getPartNum(localNodeIndex);
				if (localKeySet.get(index)==null)
					localKeySet.put(index, newUpBuilder(localIp, index, realKey, entry.getValue()));
				else
				{
					FpbPerNodeUp.Builder upBuilder= localKeySet.get(index);
					upBuilder.addUps(FpbUpdate.newBuilder().setKey(ByteString.copyFromUtf8(realKey)).
							setValue(ByteString.copyFromUtf8(entry.getValue())));
				}
			}
			else
			{
				int index = hashCode % DCManager.getPartNum(keyNode);
				if (remoteKeySet.containsKey(keyNode)){
					Map<Integer, FpbPerNodeUp.Builder> nodesUp = new HashMap<Integer,FpbPerNodeUp.Builder>();
					nodesUp.put(index, newUpBuilder(DCManager.getNodeIp(keyNode), index, realKey, entry.getValue()));
					remoteKeySet.put(keyNode, nodesUp);
				}
				else{
					Map<Integer, FpbPerNodeUp.Builder> nodesUp = remoteKeySet.get(keyNode);
					if (nodesUp.containsKey(index)) {
						FpbPerNodeUp.Builder upBuilder= nodesUp.get(index);
						upBuilder.addUps(FpbUpdate.newBuilder().setKey(ByteString.copyFromUtf8(realKey)).
								setValue(ByteString.copyFromUtf8(entry.getValue())));
					}
					else{
						nodesUp.put(index, newUpBuilder(DCManager.getNodeIp(keyNode), index, realKey, entry.getValue()));
					}
						
				}
			}
		}
		for(Map.Entry<Integer, FpbPerNodeUp.Builder> entry : localKeySet.entrySet()){
			localUpdates.addPerNodeup(entry.getValue());
		}
		
		for(Map.Entry<Integer, Map<Integer, FpbPerNodeUp.Builder>> entry1 : remoteKeySet.entrySet())
			for(Map.Entry<Integer, FpbPerNodeUp.Builder> entry2 : entry1.getValue().entrySet()){
				remoteUpdates.addPerNodeup(entry2.getValue());
		}
		
		FpbPrepTxnReq prepTxnReq = FpbPrepTxnReq.newBuilder().setTxid(txId).
				setLocalUpdates(localUpdates.build()).setRemoteUpdates(remoteUpdates.build()).build();
		
		connection = DCManager.getLocalConnection();
		connection.send(MSG_PrepTxnReq, prepTxnReq);
		FpbPrepTxnResp resp = FpbPrepTxnResp.parseFrom(connection.receive(MSG_PrepTxnResp));
		if (!resp.getSuccess())
			throw new Exception();
	}
	
	private FpbPerNodeUp.Builder newUpBuilder(String ip, int part_id, String key, String value)
	{
		return FpbPerNodeUp.newBuilder().setNode(ByteString.copyFromUtf8(ip))
				.setPartitionId(part_id + 1).addUps(FpbUpdate.newBuilder().setKey(ByteString.copyFromUtf8(key)).
						setValue(ByteString.copyFromUtf8(value)));
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
		return get(key);
	}

	public void delayedPut(Object key, Object value) {
		try {
			put(key, value);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public boolean isCoordinator() {
		// TODO Auto-generated method stub
		return true;
	}
	
	private String getKeyFromServer(Object key) {
		int keyNode, partitionId;

		AntidoteConnection connection;
		try{
			if(key instanceof MagicKey){
				log.info("Is Magic key!!");
				keyNode = ((MagicKey)key).node;
				connection = DCManager.getConnectionByKey(key);
				partitionId = key.hashCode() % DCManager.getPartNum(keyNode);
			}
			else{	
				Pair<Integer, Integer> location = DCManager.locateForNormalKey(key);
				keyNode = location.fst;
				connection = DCManager.getConectionByIndex(location.fst);
				partitionId = key.hashCode() % DCManager.getPartNum(location.snd);				
			}
			
			
			FpbReadReq readReq = FpbReadReq.newBuilder().setTxid(txId).setPartitionId(partitionId+1).
					setKey(ByteString.copyFromUtf8(((MagicKey)key).key)).build();
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
		catch (Exception e){
			System.exit(0);
			return null;
		}
		
	}

}
