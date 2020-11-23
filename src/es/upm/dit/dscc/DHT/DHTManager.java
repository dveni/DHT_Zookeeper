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
	//TODO Ver que hacer con esto:
	private String           localAddress;
	
	private TableManager      tableManager;
	private boolean           endConfigure = false;
	private DHTUserInterface  dht;
	private zkMember 		  zkMember;
	

	public DHTManager() {

		LOGGER.warning("Start of configuration of Zookeeper Cluster");
		InetAddress localhost;
		String ip;
        String hostname;
        
        //TODO
        //Falta darle un valor a localAddress, que realmente va a ser el myId de cada znode
        
		try {
			//TODO REVISAR ESTO MUY BIEN
			//127.0.0.1/nombreTerminal
			localhost = InetAddress.getLocalHost();
			//127.0.0.1
			ip = localhost.getHostAddress();
			//nombreTerminal
            hostname = localhost.getHostName();
            System.out.println("New Server: IP Addres: " + ip + " | Hostname: " + hostname);
            //TODO FALTA EL PORT --> ¿PASAR COMO PARAMETRO Y COGER EN EL MAIN???
            
            //TODO Dejo esto aqui para que funcionen el resto de clases, pero localAddres no vale nada       
            //localAddress = ip;
            
		} catch (Exception e) {
			LOGGER.severe("Error to create the Zookeeper Cluster");
		}

		if (!endConfigure) {
			configure();
		}

		LOGGER.finest("End of configuration Zookeeper Cluster");
	}

	private void configure() {
		this.mutex           = new operationBlocking();
		this.zkMember        = new zkMember(nServersMax, nReplica, tableManager);
		//TODO Asignacion de myId al localAddress
		this.localAddress = zkMember.getLocalAddress();
		
		//TODO No se si va a dar problemas crear zkMember antes que tableManager...........
		this.tableManager    = new TableManager(localAddress, nServersMax, nReplica);
		this.dht             = new DHTOperations(mutex, tableManager);
		
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




