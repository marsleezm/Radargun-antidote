package org.radargun.cachewrappers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.basho.riak.protobuf.AntidotePB.FpbNodePart;
import com.basho.riak.protobuf.AntidotePB.FpbPartList;
import com.basho.riak.protobuf.AntidotePB.FpbPartListReq;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class DCInfoManager {
	private static Log log = LogFactory.getLog(DCInfoManager.class);	
	private static final Integer MSG_PartListReq = 85;
	private static final Integer MSG_PartList = 88;
	
	static List<String> nodeNames = new ArrayList<String>();
	static List<String> ips = new ArrayList<String>();
	static List<Integer> nodePartitionNum = new ArrayList<Integer>();
	static List<Pair> nodePartList = new ArrayList<Pair>();
	static Integer nodeIndex = -1;
	static String localIp;
	
	static public void init(){
		initHashFun(); 
	}

	static public String getDefinedCacheNames() {
		// TODO Auto-generated method stub
		return null;
	}

	static public int getAddressesSize() {
		return nodeNames.size();
	}

	static public List<String> getMembers() {
		return nodeNames;
	}

	static public int getCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	public String getVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	static public Map<String, String> getStat() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object getConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	static public int getNodeIndex() {
		return nodeIndex;
	}
	
	static private void initHashFun(){
		try {
			InetAddress addr = InetAddress.getLocalHost();
			localIp = addr.getHostAddress();
			log.info("localIp is "+localIp);
			AntidoteConnection tempConnection = new AntidoteConnection(localIp);
			
			tempConnection.send(MSG_PartListReq, FpbPartListReq.newBuilder().setNoop(true).build());
			FpbPartList partList = FpbPartList.parseFrom(tempConnection.receive(MSG_PartList));
			int index = 0;
			for(FpbNodePart nodePart : partList.getNodePartsList())
			{	
				String nodeName = nodePart.getIp();
				
				//Add the number of partition
				nodePartitionNum.add(nodePart.getNumPartitions());
				
				//Create a hash function that is a list of {node, index of this node's partitions}
				for(int i=1; i<=nodePart.getNumPartitions(); ++i)
					nodePartList.add(new Pair(index,i));
				nodeNames.add(nodeName);
				String ip = nodeName.split("@")[1].replace("'", "");
				if(ip.equals(localIp) || ip.equals("127.0.0.1"))
					nodeIndex = index;
				ips.add(ip);
				++index;
			}
			log.info("All nodes are"+nodeNames);
			tempConnection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
	}

	static public int getPartNum(int index) {
		// TODO Auto-generated method stub
		return nodePartitionNum.get(index);
	}

	static public String getLocalNodeName() {
		// TODO Auto-generated method stub
		return nodeNames.get(nodeIndex);
	}

	static public String getNodeName(int index) {
		// TODO Auto-generated method stub
		return nodeNames.get(index);
	}
	
	static public Pair locateForNormalKey(Object key){
		int index = Math.abs(key.hashCode()) % nodePartList.size();
		return nodePartList.get(index);
	}

	static public List<String> getIps() {
		return ips;
	}
	
	static public void clear() {
	}

}
