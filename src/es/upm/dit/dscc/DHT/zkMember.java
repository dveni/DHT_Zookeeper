package es.upm.dit.dscc.DHT;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.io.IOException;
import java.util.Random;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper; 
import org.apache.zookeeper.data.Stat;

// This is a simple application for detecting the correct processes using ZK. 
// Several instances of this code can be created. Each of them detects the 
// valid numbers.

// Two watchers are used:
// - cwatcher: wait until the session is created. 
// - watcherMember: notified when the number of members is updated

// the method process has to be created for implement Watcher. However
// this process should never be invoked, as the "this" watcher is used

public class zkMember implements Watcher{
	
	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	private int       nReplica;
	private int       nServersMax;
	private int       nServers;
	private boolean   isQuorum       = false;
	private boolean   firstQuorum    = false;
	private boolean   pendingReplica = false;
	
	private List<String> previousZnodes = null;
	
	private String   failedServerTODO;
	
	//TODO No se si nos hace falta, directamente es myId lo mismo
	private String   localAddress;
	
	private TableManager tableManager;

	
	private static final int SESSION_TIMEOUT = 5000;

	private static String rootMembers = "/members";
	private static String aMember = "/member-";
	private String myId;
	
	// TODO VER QUE HACER CON LAS OPERACIONES
	//Nodo para las operaciones
	private static String rootOperations = "/operations";
	private static String aOperation = "/oper-";
	private String operationId;
	
	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

	private ZooKeeper zk;
	
	public zkMember (int nServersMax, int nReplica, TableManager tableManager) {
		this.nServers     = 0;
		this.nServersMax  = nServersMax;
		this.nReplica     = nReplica;
		this.tableManager = tableManager;
		
		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create a session and wait until it is created.
		// When is created, the watcher is notified
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
				try {
					// Wait for creating the session. Use the object lock
					wait();
					//zk.exists("/",false);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		} catch (Exception e) {
			System.out.println("Error");
		}

		// Add the process to the members in zookeeper

		if (zk != null) {
			// Create a folder for members and include this process/server
			try {
				// Create a folder, if it is not created
				String response = new String();
				Stat s = zk.exists(rootMembers, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootMembers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}

				// Create a znode for registering as member and get my id
				myId = zk.create(rootMembers + aMember, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

				myId = myId.replace(rootMembers + "/", "");
				this.localAddress = myId;
				
				
				List<String> list = zk.getChildren(rootMembers, watcherMember, s); //this, s);
				manageZnodes(list);
				this.nServers = list.size();
				
				LOGGER.fine("Added a server. NServers: " + nServers + " myId: " + myId);
				System.out.println("Created znode nember id:"+ myId );
				printListMembers(list);
				//TODO VER QUE HACER CON LAS OPERACIONES
				// Creamos carpeta raiz para ir introduciendo las operaciones
				s = zk.exists(rootOperations, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					zk.create(rootOperations, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}

		}
	}
	
	//Asignamos el myId a LocalAddress
	public String getLocalAddress() {
		return this.localAddress;
	}

	// Notified when the session is created
	private Watcher cWatcher = new Watcher() {
		public void process (WatchedEvent e) {
			System.out.println("Created session");
			//System.out.println(e.toString());
			notify();
		}
	};

	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
			List<String> list = zk.getChildren(rootMembers, false); //this);
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}
	
	private void printListMembers (List<String> list) {
		System.out.println("Remaining # members:" + list.size());
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();
	}
	
	//TODO VER QUE HACER CON LAS OPERACIONES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	//Metodo para crear nodos operacion [operations/oper-operationsId] TODO
	private void createOperation() {
		try {
			Stat s = zk.exists(rootOperations, false);
			// Create a znode for registering as operation and get my id
			operationId = zk.create(rootOperations + aOperation, new byte[0], 
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

			operationId = operationId.replace(rootOperations + "/", "");
			
			//Cambiar este watcher TODO
			List<String> list = zk.getChildren(rootOperations, watcherMember, s);
			System.out.println("Created znode operation id:"+ operationId );
			
		} catch (Exception e) {
			System.out.println("Error while creating operation");
			System.out.println("Exception: " + e);
		}
	}
	
	// Notified when the number of children in /member is updated
	private Watcher  watcherMember = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------Watcher Member------------------\n");		
			try {
				List<String> list = zk.getChildren(rootMembers,  watcherMember); //this);
				
				//TODO Al cambiar /rootMembers se gestiona: 
				manageZnodes(list);
				nServers = list.size();	
				printListMembers(list);
				
			} catch (Exception e) {
				System.out.println("Exception: wacherMember");
			}
		}
	};
	
	public boolean manageZnodes(List<String> newZnodes) {

		String address = null;

		// There are enough servers: nServers = nServersMax
		// Caso: Ya hay mas de 3 servidores
		if (newZnodes.size() > nServersMax) return false;

		// TODO: Handle if on servers fails before creating the first quorum
		// TODO: Currently supports one failure. Check if there are more than 1 fail
		//       Supposed that no server fails until there are quorum

		//Caso: Se quita un servidor
		if (previousZnodes != null && newZnodes.size() < previousZnodes.size()) {
			LOGGER.warning("A server has failed. There is no quorum!!!!!");
			// A server has failed
			String failedServer = crashedServer(previousZnodes, newZnodes);
			deleteServer(failedServer);
			nServers--;
			isQuorum       = false;
			pendingReplica = true;
			previousZnodes   = newZnodes;
			
			//TODO VER SI METER AQUI LA MEJORA DE LANZAR SERVERS AUTOMATICAMENTE CUANDO NO HAY QUORUM

			return false;
		}
		
		//Caso: Se añade un servidor
		if (newZnodes.size() > nServers) {

			if (nServers == 0 && newZnodes.size()>0) {
				for (Iterator<String> iterator = newZnodes.iterator(); iterator.hasNext();) {

					String itAddress = (String) iterator.next();
					addServer(itAddress);
					LOGGER.fine("Added a server. NServers: " + nServers +  "Server: " + itAddress + ".");
					if (!itAddress.equals(localAddress)) {
						//TODO
						//sendMessages.sendInit(itAddress);
					}
					if (nServers == nServersMax) {
						isQuorum    = true;
						firstQuorum = true;
					}
				}

			} else {
				if (newZnodes.size() > nServers) {
					HashMap<Integer, String> DHTServers;
					address = newZnodes.get(newZnodes.size() - 1);
					addServer(address);
					LOGGER.fine("Added a server. NServers: " + nServers + ". Server: " + address);
					if (nServers == nServersMax) {
						isQuorum    = true;
						// A server crashed and is a new one
						if (firstQuorum) {
							// A previous quorum existed. Then tolerate the fail
							// Add the new one in the DHTServer							
							String failedServer = newServer(newZnodes);
							failedServerTODO     = failedServer;
							// Add the server in DHTServer
							DHTServers = addServer(failedServer);
							if (DHTServers == null) {
								LOGGER.warning("DHTServers is null!!");
							} else {
								//TODO
								//sendMessages.sendServers(failedServer, DHTServers);
							}
							// Send the Replicas 
							//transferData(failedServer);
							pendingReplica = true;
						} else {
							firstQuorum = true;
						}
					}
				}
			}
		}
		LOGGER.fine(tableManager.printDHTServers());
		previousZnodes = newZnodes;
		return true;
	}
	
	public HashMap<Integer, String>  addServer(String address) {
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();

		if (nServers >= nServersMax) {
			return null;
		} else {
			// Find a hole
			for (int i = 0; i < nServersMax; i ++) {
				if (DHTServers.get(i) == null) {
					DHTServers.put(i, address);
					if (DHTTables.get(i) == null) {
						DHTTables.put(i, new DHTHashMap());
					}	
					nServers++;
					//sendMessages.sendServers(address, DHTServers);
					LOGGER.finest("Added a server. NServers: " + nServers);
					return DHTServers;
				}
			}
		}
		LOGGER.warning("Error: This sentence shound not run");
		return null;
	}

	public Integer deleteServer(String address) {
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		
		for (int i = 0; i < nServersMax; i ++) {
			if (address.equals(DHTServers.get(i))) {
				DHTServers.remove(i);
				return i;
			}
		}
		LOGGER.warning("This sentence should no be run");
		return null;
	}

	public String crashedServer(List<String> previousZnodes, List<String> newZnodes) {

		for (int k = 0; k < newZnodes.size(); k++) {
			if (previousZnodes.get(k).
					equals(newZnodes.get(k))) {
			} else {
				return previousZnodes.get(k);
			}

		}

		return previousZnodes.get(previousZnodes.size() - 1);

	}

	public String newServer(List<String> newZnodes) {
		return newZnodes.get(newZnodes.size() - 1);
	}
	
	public void transferData(String address) {
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();

		if (pendingReplica) {
			pendingReplica = false;
		} else {
			return;
		}

		if (!address.equals(failedServerTODO)) {
			LOGGER.severe("!!!!!!! address != failedServerTODO");			
		}

		//address = failedServerTODO;
		//TODO
		//sendMessages.sendServers(address, DHTServers);
		

		int i   = 0;
		int posNew = 0;
		for (i = 0; i < nServersMax; i++){
			//if (address.equals(DHTServers.get(i))) {
			//			Address repAddres = DHTServers.get(i);
			if (address.equals(DHTServers.get(i)) ) {
				posNew = i;
				break;
			}
		}

		int posLocal = 0;
		for (i = 0; i < nServersMax; i++){
			if (localAddress.equals(DHTServers.get(i))) {
				posLocal = i;
				break;
			}
		}

		LOGGER.fine("Check whether sending table (-1) from " + posLocal + " to " + posNew);

		int posNext = (posNew + 1) % nServersMax;
		if (posLocal == posNext) {
			LOGGER.fine("pos: " + posNew + " local: " + posLocal + " address: " + address);		
			//			sendMessages.sendReplicaData(address, dhts.get(posLocal), posLocal, posNew);
			Set <String> hashMap = DHTTables.get(posNew).keySet();
			for (Iterator<String> iterator = hashMap.iterator(); iterator.hasNext();) {
				String key = (String) iterator.next();
				LOGGER.fine("posNew + " + posNew+ " key: " + key);
				//TODO
				//sendMessages.sendPut(address, new DHT_Map(key, DHTTables.get(posNew).get(key)), true);				
			}

		}

		LOGGER.fine("Check whether sending table (0) from " + posLocal + " to " + posNew);
		// send the second replica of the previous
		for (int j = 1; j < nReplica; j++) {
			int posPrev = (posNew - j) % nServersMax;
			if (posPrev < 0) {posPrev = posPrev + nServersMax;}
			if (posLocal == posPrev) {
				LOGGER.fine("replica: " + posNext + " address: "  + address);
				LOGGER.fine("replica: " + posNext + " address: " + address);
				Set <String> hashMap = DHTTables.get(posLocal).keySet();
				//sendMessages.sendReplicaData(address, dhts.get(posLocal), posLocal, posNew);
				for (Iterator<String> iterator = hashMap.iterator(); iterator.hasNext();) {
					String key = (String) iterator.next();
					LOGGER.fine("posLocal + " + posLocal+ " key: " + key);
					//TODO
					//sendMessages.sendPut(address, new DHT_Map(key, DHTTables.get(posLocal).get(key)), true);	
				}

			}
		}

	}

	public void putReplica (int posReplica, DHTUserInterface dht) {		
		HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();
		DHTTables.put(posReplica, dht);
	}

	public void putDHTServers(HashMap <Integer, String> newDHTServers) {
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		for (int i = 0; i < nServersMax; i++) {
			DHTServers.put(i, newDHTServers.get(i));
		}

		LOGGER.fine(tableManager.printDHTServers());
	}

	public boolean isQuorum( ) {
		return isQuorum;
	}
}
