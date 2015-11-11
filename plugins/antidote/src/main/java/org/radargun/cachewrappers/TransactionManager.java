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
		//long t1 = System.nanoTime(), t2;
		connection.send(MSG_StartTxnReq, startTxnReq);
		//t2= System.nanoTime();
		//log.info("Start txn takes:"+(t2-t1));
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
			FpbSingleUpReq singleUpReq;
			AntidoteConnection connection;
			//Partition id has to be plus one because of the index in Erlang is different.
			if( key instanceof MagicKey)
			{
				MagicKey mKey = (MagicKey)key;
				connection = connections.get(mKey.node);
				Integer partitionId = toErlangIndex(Math.abs(mKey.hashCode()) % DCInfoManager.getPartNum(mKey.node));
				//if (mKey.key.startsWith("ITEM"))
				//	log.info("No transaction put magic: key is "+mKey.key+", node is "+mKey.node+", partitionid is"+partitionId);
				singleUpReq = FpbSingleUpReq.newBuilder().setKey(mKey.getKey())
						.setValue(newValue).setPartitionId(partitionId).build();
			}
			else
			{	
				Pair location = DCInfoManager.locateForNormalKey(key);
				connection = connections.get(location.fst);
				//if (((String)key).startsWith("ITEM"))
				//		log.info("No transaction put: key is "+key+", node is "+location.fst+", partitionid is "+location.snd);
				singleUpReq = FpbSingleUpReq.newBuilder().setKey((String)key)
						.setValue(newValue).setPartitionId(location.snd).build();
			}
			//t2 = System.nanoTime();
			connection.send(MSG_SingleUpReq, singleUpReq);
			FpbPrepTxnResp resp = FpbPrepTxnResp.parseFrom(connection.receive(MSG_PrepTxnResp));
			//t3 = System.nanoTime();
			//log.info("Single up takes:"+(t3-t2));
			if (resp.getResult() == 0){
				log.warn("Trying to put ["+key+","+value+"] failed!");
				throw new Exception();
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
		int localNodeIndex = DCInfoManager.getNodeIndex(),
				localPartNum = DCInfoManager.getPartNum(localNodeIndex);
		//Plus one because we won't use the first slot, to match the index style in Erlang, which starts from 1.
		FpbPerNodeUp.Builder[] localKeySet = new FpbPerNodeUp.Builder[localPartNum+1];
		Map<Pair, FpbPerNodeUp.Builder> remoteKeySet = 
				new HashMap<Pair, FpbPerNodeUp.Builder>();
		FpbNodeUps.Builder localUpdates = FpbNodeUps.newBuilder(),
				remoteUpdates = FpbNodeUps.newBuilder();
		//long t1 = System.nanoTime(), t2,t3;
		if(writeBuffer.size() == 0)
		{
			readBuffer.clear();
			writeBuffer.clear();
			return true;
		}
		
		for(Map.Entry<Object, FpbValue> entry : writeBuffer.entrySet())
		{	
			MagicKey mKey = (MagicKey)entry.getKey();
			int keyNode, hashCode;
			String realKey;
			keyNode = mKey.node;
			realKey = mKey.getKey();
			hashCode = Math.abs(mKey.hashCode());
			
			if (keyNode == localNodeIndex)
			{
				int index = toErlangIndex(hashCode % DCInfoManager.getPartNum(localNodeIndex));

				if (localKeySet[index] == null)
				{
					//if(realKey.equals("ITEM_7036"))
					//log.info("Not exist, Putting: "+ realKey+":"+index);
					localKeySet[index] = newUpBuilder(keyNode, index, realKey, entry.getValue());
				}
				else
				{
					//if(realKey.equals("ITEM_7036"))
					//log.info("Exist,Putting: "+ realKey+":"+index);
					localKeySet[index].
							addUps(FpbUpdate.newBuilder().setKey(realKey).
							setValue(entry.getValue()));
				}
			}
			else
			{
				int partIndex = toErlangIndex(hashCode % DCInfoManager.getPartNum(keyNode));
				Pair myPair = new Pair(keyNode, partIndex);
				//if(realKey.startsWith("ITEM"))
				//log.info("Remote putting: "+realKey+" : "+partIndex);
				if (remoteKeySet.containsKey(myPair) == false)
					remoteKeySet.put(myPair, newUpBuilder(keyNode, partIndex, realKey, entry.getValue()));
				else
					remoteKeySet.get(myPair).addUps(FpbUpdate.newBuilder().setKey(realKey).
							setValue(entry.getValue()));
			}
		}
		for(int i =1;  i<=localPartNum; ++i)
			if(localKeySet[i] != null)
				localUpdates.addPerNodeup(localKeySet[i]);
		
		for(Map.Entry<Pair, FpbPerNodeUp.Builder> entry1 : remoteKeySet.entrySet())
				remoteUpdates.addPerNodeup(entry1.getValue());
		
		FpbPrepTxnReq prepTxnReq = FpbPrepTxnReq.newBuilder().setTxid(txId).setThreadid(threadId).
				setLocalUpdates(localUpdates).setRemoteUpdates(remoteUpdates).build();
		
		//t2= System.nanoTime();
		//log.info("Wrap write set takes:"+(t2-t1));
		
		connection = connections.get(DCInfoManager.getNodeIndex());
		try {
			connection.send(MSG_PrepTxnReq, prepTxnReq);
			FpbPrepTxnResp resp;
			resp = FpbPrepTxnResp.parseFrom(connection.receive(MSG_PrepTxnResp));
			
			//t3= System.nanoTime();
			//log.info("Got request takes:"+(t3-t2));
			
			isInTxn = false;
			writeBuffer.clear();
			readBuffer.clear();
			if(resp.getResult()==0)
				return false;
			else
				return true;
			//	return true;
			//else
			//{
			//	log.info("Transaction failed!");
			//	return false;
			//}
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
	
	private FpbPerNodeUp.Builder newUpBuilder(int node_id, int part_id, String key, FpbValue value)
	{
		return FpbPerNodeUp.newBuilder().setNodeId(node_id)
				.setPartitionId(part_id).addUps(FpbUpdate.newBuilder().setKey(key).
						setValue(value));
	}

	public void abort() {
		// TODO Auto-generated method stub
		isInTxn = false;
		writeBuffer.clear();
		readBuffer.clear();
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
		Pair location;
		FpbReadReq.Builder builder = FpbReadReq.newBuilder();

		AntidoteConnection connection;
		try{
			if(key instanceof MagicKey){
				int keyNode = ((MagicKey)key).node;
				location = new Pair(keyNode, 
						toErlangIndex(Math.abs(key.hashCode()) % DCInfoManager.getPartNum(keyNode)));
				builder.setKey(((MagicKey)key).getKey());
				//log.info(realKey+" read: ["+keyNode+","+partitionId);
			}
			else{
				location = DCInfoManager.locateForNormalKey(key);
				builder.setKey((String)key);
				//toErlangIndex(Math.abs(key.hashCode()) % DCInfoManager.getPartNum(location.fst));
			}

			connection = connections.get(DCInfoManager.getNodeIndex());
			if(location.fst == DCInfoManager.getNodeIndex())
			{
				builder.setNodeId(location.fst);
				builder.setPartitionId(location.snd);
				//connection = connections.get(location.fst);
			}
			//Replicated read
			else if(DCInfoManager.replicateNode(location.fst))
			{
				//log.info("Replicated read: for key ["+key+"], owner is ["+location.fst
				//		+"], my index is "+DCInfoManager.getNodeIndex());
				builder.setReplicaIp(DCInfoManager.getNodeName(location.fst));
				//connection = connections.get(DCInfoManager.getNodeIndex());
			}
			else
			{
				builder.setNodeId(location.fst);
				builder.setPartitionId(location.snd);
				//connection = connections.get(location.fst);
			}

			if (isInTxn == true)
				//if (realKey.startsWith("ITEM"))
				//	log.info("In transaction get: key is "+realKey+", node is "+keyNode);
				builder.setTxid(txId);
			
			try {
				//long t2 = System.nanoTime(), t3;
				connection.send(MSG_ReadReq, builder.build());
				FpbValue V= FpbValue.parseFrom(connection.receive(MSG_Value));
				//t3 = System.nanoTime();
				//log.info("Read takes:"+(t3-t2));
				return V;
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
	
	public static int toErlangIndex(int a)
	{
		return a+1;
	}

	public boolean isInTxn() {
		// TODO Auto-generated method stub
		return isInTxn;
	}

}
