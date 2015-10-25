package org.radargun.cachewrappers;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
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
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.cachewrappers.AntidoteConnection;

public class TransactionManager {
    private AntidoteConnection connection = null;
    private static final Integer STRING_FIELD = 12;
    private static final Integer LONG_FIELD = 13;
    private static final Integer DOUBLE_FIELD = 14;
    private static final Integer NULL_FIELD = 0;
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
    private Map<Object, FpbValue> writeBuffer = new HashMap<Object, FpbValue>();
    private Map<Object, FpbValue> readBuffer = new HashMap<Object, FpbValue>();
    
    private List<AntidoteConnection> connections = new ArrayList<AntidoteConnection>();
	
    public TransactionManager(){
    	log.info("Establishing connection");
    	for (String ip : DCInfoManager.getIps())
			try {
				log.info("Ip is "+ip);
				connections.add(new AntidoteConnection(ip));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(0);
			}
    }
    
	public void begin() throws IOException {
		//log.info("Transaction started!!!");
		FpbStartTxnReq startTxnReq = FpbStartTxnReq.newBuilder().setClock(0).build();
		connection = connections.get(DCInfoManager.getNodeIndex());
		connection.send(MSG_StartTxnReq, startTxnReq);
		//log.info("Sent txn message");
		txId = FpbTxId.parseFrom(connection.receive(MSG_TxId));
		//log.info("Got txn id");
		isInTxn = true;
	}
	
	public void put(Object key, Object value) throws Exception {
		//log.info("Trying to put "+key+": value is "+value);
		FpbValue newValue;
		if (value instanceof String)
			newValue = FpbValue.newBuilder().setField(STRING_FIELD).setStrValue((String)value).build();
		else if(value instanceof Long || value instanceof Integer)
			newValue = FpbValue.newBuilder().setField(LONG_FIELD).setLongValue((Long)value).build();
		else if(value instanceof Double || value instanceof Float)
			newValue = FpbValue.newBuilder().setField(DOUBLE_FIELD).setDoubleValue((Double)value).build();
		else{
			newValue = (FpbValue)value;
		}
		
		if (isInTxn)
			writeBuffer.put(key, newValue);
		else{
			//Partition id has to be plus one because of the index in Erlang is different.
			if( key instanceof MagicKey)
			{
				MagicKey mKey = (MagicKey)key;
				AntidoteConnection connection = connections.get(mKey.node);
				Integer partitionId = Math.abs(mKey.hashCode()) % DCInfoManager.getPartNum(mKey.node);
				if (mKey.key.startsWith("ITEM"))
					log.info("No transaction put magic: key is "+mKey.key+", node is "+mKey.node+", partitionid is"+partitionId);
				FpbSingleUpReq singleUpReq = FpbSingleUpReq.newBuilder().setKey(mKey.key)
						.setValue(newValue).setPartitionId(partitionId+1).build();
				
				connection.send(MSG_SingleUpReq, singleUpReq);
				FpbPrepTxnResp resp = FpbPrepTxnResp.parseFrom(connection.receive(MSG_PrepTxnResp));
				if (!resp.getSuccess())
					throw new Exception();
			}
			else
			{	
				Pair<Integer, Integer> location = DCInfoManager.locateForNormalKey(key);
				AntidoteConnection connection = connections.get(location.fst);
				if (((String)key).startsWith("ITEM"))
						log.info("No transaction put: key is "+key+", node is "+location.fst+", partitionid is "+location.snd);
				FpbSingleUpReq singleUpReq = FpbSingleUpReq.newBuilder().setKey((String)key)
						.setValue(newValue).setPartitionId(location.snd+1).build();
				
				connection.send(MSG_SingleUpReq, singleUpReq);
				FpbPrepTxnResp resp = FpbPrepTxnResp.parseFrom(connection.receive(MSG_PrepTxnResp));
				if (resp.getSuccess() == false){
					log.warn("Trying to put ["+key+","+value+"] failed!");
					throw new Exception();
				}
			}
		}
	}

	public Object get(Integer processIndex, Object key) {
		log.trace("Trying to get "+key);
		FpbValue value;
		if(isInTxn)
		{
			value = writeBuffer.get(key);
			if( value == null )
			{
				value = readBuffer.get(key);
				if ( value == null)
				{
					value = getKeyFromServer(key, isInTxn);
					readBuffer.put(key, value);
				}
			}
		}
		else{
			value = getKeyFromServer(key, isInTxn);
		}
		
		//log.trace("Key is "+ key +", value is "+value.toString());
		
		if(value.getField() == STRING_FIELD)
			return value.getStrValue();
		else if(value.getField() == LONG_FIELD)
			return value.getLongValue();
		else if(value.getField() == DOUBLE_FIELD)
			return value.getDoubleValue();
		else if(value.getField() == NULL_FIELD)
			return null;
		else
			return value;
	}

	public boolean commit(int threadId) {
		int localNodeIndex = DCInfoManager.getNodeIndex();
		String localName = DCInfoManager.getLocalNodeName();
		Map<Integer, FpbPerNodeUp.Builder> localKeySet = 
				new HashMap<Integer, FpbPerNodeUp.Builder>();
		Map<Integer, Map<Integer, FpbPerNodeUp.Builder>> remoteKeySet = 
				new HashMap<Integer, Map<Integer, FpbPerNodeUp.Builder>>();
		FpbNodeUps.Builder localUpdates = FpbNodeUps.newBuilder(),
				remoteUpdates = FpbNodeUps.newBuilder();
		try{
		for(Map.Entry<Object, FpbValue> entry : writeBuffer.entrySet())
		{	
			MagicKey mKey = (MagicKey)entry.getKey();
			int keyNode, hashCode;
			String realKey;
			keyNode = mKey.node;
			realKey = mKey.key;
			hashCode = Math.abs(mKey.hashCode());
			
			if (keyNode == localNodeIndex)
			{
				int index = hashCode % DCInfoManager.getPartNum(localNodeIndex);
				if(realKey.startsWith("ITEM") && (threadId == 3 || threadId ==4))
					log.trace(realKey+" Local index is "+keyNode+",part index is"+index);
				if (localKeySet.get(index)==null)
					localKeySet.put(index, newUpBuilder(localName, index, realKey, entry.getValue()));
				else
				{
					FpbPerNodeUp.Builder upBuilder= localKeySet.get(index);
					upBuilder.addUps(FpbUpdate.newBuilder().setKey(realKey).
							setValue((entry.getValue())));
				}
			}
			else
			{
				int partIndex = hashCode % DCInfoManager.getPartNum(keyNode);
				if(threadId ==0)
					log.trace("Remote!: Local index is "+keyNode+"partNum is "+DCInfoManager.getPartNum(keyNode)
						+"part index is"+partIndex);
				if (remoteKeySet.containsKey(keyNode) == false){
					Map<Integer, FpbPerNodeUp.Builder> nodesUp = new HashMap<Integer,FpbPerNodeUp.Builder>();
					nodesUp.put(partIndex, newUpBuilder(DCInfoManager.getNodeName(keyNode), partIndex, realKey, entry.getValue()));
					remoteKeySet.put(keyNode, nodesUp);
				}
				else{
					Map<Integer, FpbPerNodeUp.Builder> nodesUp = remoteKeySet.get(keyNode);
					if (nodesUp.containsKey(partIndex)) {
						FpbPerNodeUp.Builder upBuilder= nodesUp.get(partIndex);
						upBuilder.addUps(FpbUpdate.newBuilder().setKey(realKey).
								setValue(entry.getValue()));
					}
					else{
						nodesUp.put(partIndex, newUpBuilder(DCInfoManager.getNodeName(keyNode), partIndex, realKey, entry.getValue()));
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
		
		FpbPrepTxnReq prepTxnReq = FpbPrepTxnReq.newBuilder().setTxid(txId).setThreadid(threadId).
				setLocalUpdates(localUpdates.build()).setRemoteUpdates(remoteUpdates.build()).build();
		
		connection = connections.get(DCInfoManager.getNodeIndex());
		try {
			connection.send(MSG_PrepTxnReq, prepTxnReq);
			FpbPrepTxnResp resp;
			resp = FpbPrepTxnResp.parseFrom(connection.receive(MSG_PrepTxnResp));
			isInTxn = false;
			if(resp.getSuccess())
				return true;
			else
			{
				log.info("Transaction failed!");
				return false;
			}
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			log.warn("Invalid protocol buffer");
			e.printStackTrace();
			System.exit(0);
			return false;
		}
		   catch (IOException e) {
			// TODO Auto-generated catch block
			log.warn("IOException");
			e.printStackTrace();
			System.exit(0);
			return false;
		} 
		}
		   catch (Exception e) {
			// TODO Auto-generated catch block
			log.warn("IOException");
			e.printStackTrace();
			System.exit(0);
			return false;
		  }
	}
	
	private FpbPerNodeUp.Builder newUpBuilder(String nodeName, int part_id, String key, FpbValue value)
	{
		return FpbPerNodeUp.newBuilder().setNode(nodeName)
				.setPartitionId(part_id + 1).addUps(FpbUpdate.newBuilder().setKey(key).
						setValue(value));
	}

	public void abort() {
		// TODO Auto-generated method stub
		isInTxn = false;
		log.warn("Trying to abort: something is wrong!!!");
	}

	public Object getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	public void delayedComputation() {
		// TODO Auto-generated method stub
		
	}

	public Object delayedGet(Object key) {
		return get(0, key);
	}
	
	public void stopConnections(){
		for(AntidoteConnection connection : connections)
			connection.close();
	}

	public void delayedPut(Object key, Object value) {
		try {
			put(key, value);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
	}

	public boolean isCoordinator() {
		// TODO Auto-generated method stub
		return true;
	}
	
	
	private FpbValue getKeyFromServer(Object key, boolean isInTxn) {
		int keyNode, partitionId;
		String realKey;

		AntidoteConnection connection;
		try{
			if(key instanceof MagicKey){
				keyNode = ((MagicKey)key).node;
				realKey = ((MagicKey) key).key;
				connection = connections.get(((MagicKey)key).node);
				partitionId = Math.abs(key.hashCode()) % DCInfoManager.getPartNum(keyNode);
				if (realKey.startsWith("ITEM"))
					log.info("No transaction get: key is "+realKey+", node is "+keyNode+", partitionid is"+partitionId);
			}
			else{
				Pair<Integer, Integer> location = DCInfoManager.locateForNormalKey(key);
				keyNode = location.fst;
				realKey = (String)key;
				connection = connections.get(location.fst);
				partitionId = Math.abs(key.hashCode()) % DCInfoManager.getPartNum(location.fst);
			}
			
			FpbReadReq readReq;
			if (isInTxn == true)
				readReq = FpbReadReq.newBuilder().setTxid(txId).setPartitionId(partitionId+1).
					setKey(realKey).build();
			else
				readReq = FpbReadReq.newBuilder().setPartitionId(partitionId+1).
				setKey(realKey).build();
			
			try {
				connection.send(MSG_ReadReq, readReq);
				return FpbValue.parseFrom(connection.receive(MSG_Value));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			} 
		}
		catch (Exception e){
			e.printStackTrace();
			System.exit(0);
			return null;
		}
		
	}

	public boolean isInTxn() {
		// TODO Auto-generated method stub
		return isInTxn;
	}

}
