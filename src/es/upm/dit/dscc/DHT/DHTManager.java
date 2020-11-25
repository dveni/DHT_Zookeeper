 package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class DHTManager implements DHTUserInterface {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;

	private int               nServersMax  = 3;
	private int               nReplica     = 2;
	
	private operationBlocking mutex;
	
	private String           localAddress;
	
	private TableManager      tableManager;
	private boolean           endConfigure = false;
	private DHTUserInterface  dht;
	private zkMember 		  zkMember;
	

	public DHTManager() {

		LOGGER.fine("Start of configuration of the new Server | Zookeeper Cluster");
		InetAddress localhost;
		String ip;
        String hostname;
        
		try {
			//TODO REVISAR ESTO MUY BIEN << BORRAR, NO VALE PARA NADA
			//127.0.0.1/nombreTerminal
			localhost = InetAddress.getLocalHost();
			//127.0.0.1
			ip = localhost.getHostAddress();
			//nombreTerminal
            hostname = localhost.getHostName();
            System.out.println("New Server: IP Addres: " + ip + " | Hostname: " + hostname);
            //TODO FALTA EL PORT --> ¿PASAR COMO PARAMETRO Y COGER EN EL MAIN???
            
		} catch (Exception e) {
			LOGGER.severe("Error to create the Zookeeper Cluster");
		}

		if (!endConfigure) {
			configure();
		}
		
		this.localAddress = zkMember.getLocalAddress();
		this.tableManager.setLocalAddress(this.localAddress);

		LOGGER.fine("End of configuration of the new Server | Zookeeper Cluster | LocalAddress: " + this.localAddress);
	}

	private void configure() {
		this.mutex           = new operationBlocking();
		this.tableManager    = new TableManager(localAddress, nServersMax, nReplica);	
		this.zkMember        = new zkMember(nServersMax, nReplica, tableManager);		
		this.dht             = new DHTOperations(mutex, tableManager, nReplica);
		this.endConfigure    = true;
	}
	
	public boolean isQuorum() {
		return zkMember.isQuorum();
	}
	
	public Integer put(DHT_Map map) {
		return dht.put(map);
	}
	
	public Integer putMsg(DHT_Map map) {
		return null;
	}
	
	public Integer get(String key) {
		return dht.get(key);
	}

	public Integer remove(String key) {
		return dht.remove(key);
	}
	
	public Integer removeMsg(String key) {
		return null;
	}
	
	public boolean containsKey(String key) {
		return dht.containsKey(key);
	}

	public Set<String> keySet() {
		return dht.keySet();
	}

	public ArrayList<Integer> values() {
		return dht.values();
	}
	
	public String getServers() {
		return tableManager.printDHTServers();
	}
	
	@Override
	public String toString() {
		return dht.toString();
	}
}




