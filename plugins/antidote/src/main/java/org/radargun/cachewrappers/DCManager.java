package org.radargun.cachewrappers;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.basho.riak.protobuf.AntidotePB.FpbNodePart;
import com.basho.riak.protobuf.AntidotePB.FpbPartList;
import com.basho.riak.protobuf.AntidotePB.FpbPartListReq;
import com.basho.riak.protobuf.AntidotePB.FpbTxId;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DCManager {
	static List<String> allNodes = new ArrayList<String>();
	static List<AntidoteConnection> connections = new ArrayList<AntidoteConnection>();
	static List<Integer> nodePartitionNum = new ArrayList<Integer>();
	static List<Pair<Integer, Integer>> nodePartList = new ArrayList<Pair<Integer, Integer>>();
	static AntidoteConnection localConnection;
	static Integer nodeIndex;
	static String localIp;
	private static Log log = LogFactory.getLog(DCManager.class);
	
	private static final Integer MSG_PartListReq = 85;
	private static final Integer MSG_PartList = 88;
	
	static public void init(String configFile){
		try {
			InetAddress addr = InetAddress.getLocalHost();
			localIp = addr.getHostAddress();
			log.warn("Local Ip is "+localIp);
			localConnection = new AntidoteConnection(localIp);
			getHashFun();
			nodeIndex = allNodes.indexOf(localIp);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} 
	}
	
	static public AntidoteConnection getConnectionByKey(Object key){
		return connections.get(((MagicKey)key).node);
	}

	static public String getDefinedCacheNames() {
		// TODO Auto-generated method stub
		return null;
	}

	static public int getAddressesSize() {
		return allNodes.size();
	}

	static public List<String> getMembers() {
		return allNodes;
	}

	static public void stop() {
		localConnection.close();
		for(AntidoteConnection connection : connections)
			connection.close();
	}

	static public int getCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	static public void clear() {
		for(AntidoteConnection connection : connections)
			connection.close();
	}

	static public String getVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	static public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	static public Map<String, String> getStat() {
		// TODO Auto-generated method stub
		return null;
	}

	static public Object getConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	static public int getNodeIndex() {
		return nodeIndex;
	}

	static public AntidoteConnection getLocalConnection() {
		return localConnection;
	}
	
	private void parseXML(String config) {
		/*DocumentBuilderFactory dbFactory 
        	= DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(config);
		doc.getDocumentElement().normalize();
		NodeList nList = doc.getElementsByTagName("node");
		for (int i = 0; i < nList.getLength(); ++i) {
            Node nNode = nList.item(i);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
               Element element = (Element) nNode;
               String ip = element.getAttribute("ip");
               allNodes.add(ip);
               connections.put(ip, new AntidoteConnection(ip));
            }
         }
         */
	}
	
	static private void getHashFun(){
		try {
			localConnection.send(MSG_PartListReq, FpbPartListReq.newBuilder().setNoop(true).build());
			FpbPartList partList = FpbPartList.parseFrom(localConnection.receive(MSG_PartList));
			int currentNodeIndex = 0;
			for(FpbNodePart nodePart : partList.getNodePartsList())
			{	
				String nodeName = nodePart.getIp().toStringUtf8();
				nodePartitionNum.add(nodePart.getNumPartitions());
				for(int i=0; i<nodePart.getNumPartitions(); ++i)
				{
					nodePartList.add(new Pair<Integer, Integer>(currentNodeIndex,i));
					log.info("nodePart :"+nodePartList.toString());
				}
				allNodes.add(nodeName);
				log.warn("ip:"+nodeName+", partitions:"+nodePart.getNumPartitions());
				String ip = nodeName.split("@")[1].replace("'", "");
				if(ip.equals(localIp) == false)
					connections.add(new AntidoteConnection(ip));
				++currentNodeIndex;
			}
			log.info("Allnodes are"+allNodes);
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

	static public String getLocalNodeIp() {
		// TODO Auto-generated method stub
		return localIp;
	}

	static public List<String> getAddresses() {
		return allNodes;
	}

	static public String getNodeIp(int index) {
		// TODO Auto-generated method stub
		return allNodes.get(index);
	}

	static public AntidoteConnection getConectionByIndex(int index) {
		// TODO Auto-generated method stub
		return connections.get(index);
	}
	
	static public Pair<Integer, Integer> locateForNormalKey(Object key){
		int index = key.hashCode() % nodePartList.size();
		return nodePartList.get(index);
	}

}
