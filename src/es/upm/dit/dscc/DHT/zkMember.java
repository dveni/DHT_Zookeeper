package es.upm.dit.dscc.DHT;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class zkMember implements Watcher {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	private int nReplica;
	private int nServersMax;
	private int nServers;
	private boolean isQuorum = false;
	private boolean firstQuorum = false;
	private boolean pendingReplica = false;
	private List<String> previousZnodes = null;
	private String failedServerTODO;
	private String localAddress;
	private TableManager tableManager;
	private DHTUserInterface dht;
	
	//DATA
	private HashMap<Integer, String> backupServers;
	private HashMap<Integer, DHTUserInterface> backupTables;
	
	//RUTAS ZOOKEEPER
	//MEMBERS	
	private static final int SESSION_TIMEOUT = 5000;
	private static String rootMembers = "/members";
	private static String aMember = "/member-";
	private String myId;
	private Integer position;
	//OPERATIONS
	private static String rootOperations = "/operations";
	//LOCK
	private static String lockPath = "/locknode";
	private static String guidLock = "/guid-lock-";
	private static Integer mutex = -10;
	private String lockId;
	private static String leaderPath;
	//DATA
	private static String dhtServers = "/DHTServers";
	private static String dhtTables = "/DHTTables";

	//ZOOKEEPER
	private ZooKeeper zk;
	String[] hosts = { "127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181" };

	//CONSTRUCTOR
	public zkMember(int nServersMax, int nReplica, TableManager tableManager, DHTUserInterface dht) {
		this.nServers = 0;
		this.nServersMax = nServersMax;
		this.nReplica = nReplica;
		this.tableManager = tableManager;
		this.dht = dht;
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// 1: CREATE SESSION AND REMOVE OLD DATA/ZNODES
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
				try {
					wait();
				} catch (Exception e) {
				}
				Stat estado = zk.exists(rootMembers, false);
				//Reset: solo si no hay ningun znode
				List<String> listatmp = zk.getChildren(rootMembers, false, estado);
				if (listatmp.size()==0) {
					resetZK(zk);
				}	
			}
		} catch (Exception e) {
			System.out.println("Error");
		}
		

		// 2: CONFIGURE ZOOKEEPER CLUSTER
		if (zk != null) {
			try {
				//2.1: Guardamos en /dhtServers DHTServers (para mantener el orden)
				Stat s2 = zk.exists(dhtServers, false);
				Stat s3 = zk.exists(dhtTables, false);
				if (s2 == null && s3 == null) {
					zk.create(dhtServers, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					zk.create(dhtTables, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				} else {
					this.backupServers = getServersFromZnode();
					this.backupTables = getTablesFromZnode();
					putDHTServers(backupServers, backupTables);
				}
				
				//2.2: Creamos /rootMembers
				Stat s = zk.exists(rootMembers, false);
				if (s == null) {
					zk.create(rootMembers, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				
				//Creamos el member-myId y asignamos a localAddress
				myId = zk.create(rootMembers + aMember, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				myId = myId.replace(rootMembers + "/", "");
				this.localAddress = myId;
				this.tableManager.setLocalAddress(myId);
				
				
				//2.3 Añadimos el nuevo znode a todas las tablas con manageZnodes
				List<String> list = zk.getChildren(rootMembers, false, s);
				manageZnodes(list);
				
				//Ponemos watcher para gestionar los servidores nuevos
				s = zk.exists(rootMembers, false);
				list = zk.getChildren(rootMembers, watcherMember, s);
				
				System.out.println("--------------------------------------------------------------------------------------");
				System.out.println("|            Zookeeper Cluster   |   NEW Znode   |   [ id: " + myId + " ]       |");
				System.out.println("--------------------------------------------------------------------------------------");
				System.out.println("Znodes in Zookeeper Cluster # members:" + list.size());
				printListMembers(list);
				
				position = tableManager.getPosition(myId);
				
				// BORRAR, NO SE UTILIZA
				//TODO Ponemos un watcher apuntando al rootMembers para cuando se añada un server al que mandarle la info
				//s = zk.exists(rootMembers, false);
				//zk.getData(rootMembers, watcherTransferData, s);
				
				
				//2.4 Solo lo ejecuta el primer servidor del cluster para actualizar los backups
				if (isLeader()) {
					setServersToZnode(tableManager.getDHTServers());
					setTablesToZnode(tableManager.getDHTTables());
				}
				
				//Configuracion para poner watcher sobre /rootoperations
				configOps();
				
				LOGGER.fine(tableManager.printDHTServers());
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}
		}
	}
	
	//Metodo para resetear el estado del zookeeper
	private void resetZK(ZooKeeper zk) {
		try {
			System.out.println("Removing old znodes and old data...");
			Stat s1 = zk.exists(rootMembers, false);
			if (s1 != null) {
				zk.delete(rootMembers, s1.getVersion());
			}
			Stat s2 = zk.exists(dhtServers, false);
			if (s2 != null) {
				zk.delete(dhtServers, s2.getVersion());
			}
			Stat s3 = zk.exists(rootOperations, false);
			if (s3 != null) {
				zk.delete(rootOperations, s3.getVersion());
			}
			Stat s4 = zk.exists(lockPath, false);
			if (s4 != null) {
				zk.delete(lockPath, s4.getVersion());
			}	
			Stat s5 = zk.exists(dhtTables, false);
			if (s5 != null) {
				zk.delete(dhtTables, s5.getVersion());
			}
		} catch (Exception e) {
			LOGGER.warning("ERROR WHILE RESETING DATA IN ZOOKEEPER");	
		}	
	}
	
	// Asignamos el myId a LocalAddress
	public String getLocalAddress() {
		return this.localAddress;
	}

	// Notified when the session is created
	private Watcher cWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			System.out.println("Created session");
			notify();
		}
	};

	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}

	private void printListMembers(List<String> list) {
		Collections.sort(list);
		for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");
		}
		System.out.println();
		System.out.println("--------------------------------------------------------------------------------------");
	}
	
	// Notified when the number of children in /member is updated
	private Watcher watcherMember = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------------------------Watcher Member------------------------------------\n");
			try {
				List<String> list = zk.getChildren(rootMembers, watcherMember);
				manageZnodes(list);
				if (isLeader()) {
					setServersToZnode(tableManager.getDHTServers());
					setTablesToZnode(tableManager.getDHTTables());
				}
				LOGGER.fine(tableManager.printDHTServers());
				System.out.println("Znodes in Zookeeper Cluster # members:" + list.size());
				printListMembers(list);
				System.out.println("--------------------------------------------------------------------------------------");
				System.out.println(">>> Enter option: 1) Put. 2) Get. 3) Remove. 4) ContainKey  5) Values 7) Init 0) Exit");
			} catch (Exception e) {
				System.out.println("Exception: watcherMember");
			}
		}
	};
	
	// TODO: Ver donde lo mandamos BORRAR, NO SE UTILIZA
	// WatcherTransferData: cuando se añade un server y queremos mandarle los datos
	private Watcher watcherTransferData = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------------------------Transfer Data------------------------------------\n");
			try {
				//Asi conseguimos que solo envie datos un servidor
				if (isLeader()) {
					//Obtenemos direccion a la que enviar, y la eliminamos del rootMember
					String newServer = getAddressFromZnode();
					System.out.println("There is a new Server. Sending data to: " + newServer + " | Zookeeper Cluster");		
					transferData(newServer);		
				}
				//Configuramos el watcher para el siguiente
				Stat s = zk.exists(rootMembers, false);
				zk.getData(rootMembers, watcherTransferData, s);
				System.out.println(">>> Enter option: 1) Put. 2) Get. 3) Remove. 4) ContainKey  5) Values 7) Init 0) Exit");
			} catch (Exception e) {
				System.out.println("Exception: watcherTransferData");
			}
		}
	};

	// Metodo para que un server añada su myId a rootMember y pueda recibir info
	private void setAddressToZnode(String address) {
		byte[] byteArrayAddress = null;
		try {
			byteArrayAddress = address.getBytes();
		} catch (Exception e) {
			System.out.println("Error: setAddressToZnode");
			System.out.println("Error while serializing object");
			System.out.println("Exception: " + e);
		}
		try {
			Stat s = zk.exists(rootMembers, false);
			zk.setData(rootMembers, byteArrayAddress, s.getVersion());
		} catch (Exception e) {
			System.out.println("Error: setAddressToZnode");
			System.out.println("Error while setting data to ZK");
			System.out.println("Exception: " + e);
		}
	}

	// Metodo para conseguir el myId almacenado en rootMember y se envie info a este server
	private String getAddressFromZnode() {
		Stat s = new Stat();
		byte[] data = null;
		String address = new String();
		try {
			s = zk.exists(rootMembers, false);
			data = zk.getData(rootMembers, false, s);
		} catch (Exception e) {
			System.out.println("Error: getAddressFromZnode");
			System.out.println("Error while getting data from ZK");
			System.out.println("Exception: " + e);
		}
		if (data != null) {
			try {
				address = new String(data);
			} catch (Exception e) {
				System.out.println("Error: getAddressFromZnode");
				System.out.println("Error while deserializing object");
				System.out.println("Exception: " + e);
			}	
		}
		return address;
	}

	// Metodos para controlar coherencia de DHTServers con Zookeeper
	private void setServersToZnode(HashMap<Integer, String> DHTServers) {
		byte[] byteDHTServers = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(DHTServers);
			oos.flush();
			byteDHTServers = bos.toByteArray();
		} catch (Exception e) {
			System.out.println("Error: setServersToZnode");
			System.out.println("Error while serializing object");
			System.out.println("Exception: " + e);
		}
		try {
			Stat s = zk.exists(dhtServers, false);
			zk.setData(dhtServers, byteDHTServers, s.getVersion());
		} catch (Exception e) {
			System.out.println("Error: setServersToZnode");
			System.out.println("Error while setting data to ZK");
			System.out.println("Exception: " + e);
		}
	}

	// Metodos para controlar coherencia de DHTServers con Zookeeper
	private HashMap<Integer, String> getServersFromZnode() {
		Stat s = new Stat();
		byte[] data = null;
		HashMap<Integer, String> DHTServersFromZnode = new HashMap<Integer, String>();
		try {
			s = zk.exists(dhtServers, false);
			data = zk.getData(dhtServers, false, s);
		} catch (Exception e) {
			System.out.println("Error: getServersFromZnode");
			System.out.println("Error while getting data from ZK");
			System.out.println("Exception: " + e);
		}
		// Deserialize: Convert an array of Bytes in an operation.
		if (data != null) {
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(data);
				ObjectInputStream in = new ObjectInputStream(bis);
				DHTServersFromZnode = (HashMap<Integer, String>) in.readObject();
			} catch (Exception e) {
				System.out.println("Error: getServersFromZnode");
				System.out.println("Error while deserializing object");
				System.out.println("Exception: " + e);
			}
		}
		return DHTServersFromZnode;
	}

	// Metodos para controlar coherencia de DHTServers con Zookeeper
	private void setTablesToZnode(HashMap<Integer, DHTUserInterface> DHTTables) {
		byte[] byteDHTTables = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(DHTTables);
			oos.flush();
			byteDHTTables = bos.toByteArray();
		} catch (Exception e) {
			System.out.println("Error: setTablesToZnode");
			System.out.println("Error while serializing object");
			System.out.println("Exception: " + e);
		}
		try {
			Stat s = zk.exists(dhtTables, false);
			zk.setData(dhtTables, byteDHTTables, s.getVersion());
		} catch (Exception e) {
			System.out.println("Error: setTablesToZnode");
			System.out.println("Error while setting data to ZK");
			System.out.println("Exception: " + e);
		}
	}

	// Metodos para controlar coherencia de DHTServers con Zookeeper
	private HashMap<Integer, DHTUserInterface> getTablesFromZnode() {
		Stat s = new Stat();
		byte[] data = null;
		HashMap<Integer, DHTUserInterface> DHTTablesFromZnode = new HashMap<Integer, DHTUserInterface>();
		try {
			s = zk.exists(dhtTables, false);
			data = zk.getData(dhtTables, false, s);
		} catch (Exception e) {
			System.out.println("Error: getTablesFromZnode");
			System.out.println("Error while getting data from ZK");
			System.out.println("Exception: " + e);
		}
		// Deserialize: Convert an array of Bytes in an operation.
		if (data != null) {
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(data);
				ObjectInputStream in = new ObjectInputStream(bis);
				DHTTablesFromZnode = (HashMap<Integer, DHTUserInterface>) in.readObject();
			} catch (Exception e) {
				System.out.println("Error: getTablesFromZnode");
				System.out.println("Error while deserializing object");
				System.out.println("Exception: " + e);
			}
		}
		return DHTTablesFromZnode;
	}

	public boolean manageZnodes(List<String> newZnodes) {
		String address = null;
		// Caso: Se quita un servidor
		if (previousZnodes != null && newZnodes.size() < previousZnodes.size()) {
			LOGGER.warning("A server has failed. There is no quorum!!!!!");
			// A server has failed
			Collections.sort(newZnodes);
			String failedServer = crashedServer(previousZnodes, newZnodes);
			deleteServer(failedServer);
			nServers--;
			isQuorum = false;
			pendingReplica = true;
			previousZnodes = newZnodes;

			// TODO MEJORA DE LANZAR SERVERS AUTOMATICAMENTE CUANDO NO HAY QUORUM (PERO LO HA HABIDO PREVIAMENTE)
			// DESCOMENTAR
			
			if (firstQuorum && nServers < 3) {
				if (isLeader()) {
					try {
						System.out.println("Zookeeper Cluster | Starting a new server...");
						String[] command = { "gnome-terminal", "--window", "-x", "bash", "-c",
								"java es.upm.dit.dscc.DHT.DHTMain; exec bash" };
								//"java -jar nombreDelJar.jar; exec bash" };
						Process proc = new ProcessBuilder(command).start();
					} catch (IOException e) {
						System.out.println("Exception starting automatic server...: " + e);
					}
				}
			}	
			return false;
			
		} else {
			// Caso: Se añade un servidor
			// Aqui ponemos todos los casos que pueden suceder cuando se añade un servidor nuevo

			// Caso: si ya hay 3 servidores (nServersMax) y newZNodes es mayor, no se hace nada
			if (newZnodes.size() > nServersMax) {
				return false;
			}
			// Caso generico de añadir un servidor (newZnodes.size() > nServers)
			else {
				// Caso: se añade un servidor al cluster y le actualizamos su tabla DHTServers
				if (nServers == 0 && newZnodes.size() > 0) {
					for (Iterator<String> iterator = newZnodes.iterator(); iterator.hasNext();) {
						String itAddress = (String) iterator.next();
						addServer(itAddress);
						LOGGER.fine("Added a server. NServers: " + nServers + " | Server: " + itAddress + " | Zookeeper Cluster");
						HashMap<Integer, String> tmp = tableManager.getDHTServers();
						int validos = 0;
						for(int l=0; l < tmp.size(); l++) {
							if(tmp.get(l)!= null) {
								validos++;
							}
						}
						nServers = validos;
						if (nServers == nServersMax) {
							isQuorum = true;
							firstQuorum = true;
							System.out.println("THERE IS QUORUM | NOW, YOU CAN DO OPERATIONS | Zookeeper Cluster");
						}
					}
				} else {
					// Caso: se añade un servidor, y un servidor ya existente actualiza su tabla
					HashMap<Integer, String> DHTServers;
					Collections.sort(newZnodes);
					address = newZnodes.get(newZnodes.size() - 1);
					addServer(address);
					LOGGER.fine("Added a server. NServers: " + nServers + ". Server: " + address);
					if (nServers == nServersMax) {
						isQuorum = true;
						System.out.println("THERE IS QUORUM | NOW, YOU CAN DO OPERATIONS | Zookeeper Cluster");
						// A server crashed and is a new one
						if (firstQuorum) { //TODO CREO QUE ESTO NO VALE PARA NADA
							// A previous quorum existed. Then tolerate the fail
							// Add the new one in the DHTServer
							String failedServer = newServer(newZnodes);
							failedServerTODO = failedServer;
							// Add the server in DHTServer
							DHTServers = addServer(failedServer);
							if (DHTServers == null) {
								LOGGER.warning("DHTServers is null!!");
							}
							// Send the Replicas
							// TODO AQUI PONER EL WATCHER PARA ENVIAR DATOS AL NUEVO SERVER
							// ESTO SOLO HAY QUE HACERLO SI CAMBIAMOS LA FORMA DE ENVIAR LAS TABLAS
							//setAddressToZnode(failedServer);
							pendingReplica = true;
						} else {
							firstQuorum = true;
						}
					}
				}
			}
		}
		previousZnodes = newZnodes;
		return true;
	}

	public HashMap<Integer, String> addServer(String address) {
		Boolean existe = false;
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();

		if (nServers >= nServersMax) {
			return null;
		} else {
			//Check first
			for (int i = 0; i < nServersMax; i++) {
				String tmp = DHTServers.get(i);
				if (Objects.equals(tmp, address)) {
					existe=true;
				}
			}
			if (!existe) {
				// Find a hole
				for (int i = 0; i < nServersMax; i++) {
					if (DHTServers.get(i) == null) {
						DHTServers.put(i, address);
						if (DHTTables.get(i) == null) {
							DHTTables.put(i, new DHTHashMap());
						}
						nServers++;
						return DHTServers;
					}
				}
			}
		}
		LOGGER.warning("Error: This sentence shound not run");
		return null;
	}

	public Integer deleteServer(String address) {
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		for (int i = 0; i < nServersMax; i++) {
			if (address.equals(DHTServers.get(i))) {
				DHTServers.remove(i);
				return i;
			}
		}
		return null;
	}

	public String crashedServer(List<String> previousZnodes, List<String> newZnodes) {
		for (int k = 0; k < newZnodes.size(); k++) {
			if (previousZnodes.get(k).equals(newZnodes.get(k))) {
			} else {
				return previousZnodes.get(k);
			}
		}
		return previousZnodes.get(previousZnodes.size() - 1);
	}

	public String newServer(List<String> newZnodes) {
		return newZnodes.get(newZnodes.size() - 1);
	}
	
	//TODO: BORRAR NO SE UTILIZA
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

		int i = 0;
		int posNew = 0;
		for (i = 0; i < nServersMax; i++) {
			if (address.equals(DHTServers.get(i))) {
				posNew = i;
				break;
			}
		}

		int posLocal = 0;
		for (i = 0; i < nServersMax; i++) {
			if (localAddress.equals(DHTServers.get(i))) {
				posLocal = i;
				break;
			}
		}
		LOGGER.fine("Check whether sending table (-1) from " + posLocal + " to " + posNew);
		int posNext = (posNew + 1) % nServersMax;
		if (posLocal == posNext) {
			LOGGER.fine("pos: " + posNew + " local: " + posLocal + " address: " + address);
			Set<String> hashMap = DHTTables.get(posNew).keySet();
			for (Iterator<String> iterator = hashMap.iterator(); iterator.hasNext();) {
				String key = (String) iterator.next();
				LOGGER.fine("posNew + " + posNew + " key: " + key);
				// TODO desde aqui llamar a las operaciones??
				// sendMessages.sendPut(address, new DHT_Map(key, DHTTables.get(posNew).get(key)), true);
			}
		}
		LOGGER.fine("Check whether sending table (0) from " + posLocal + " to " + posNew);
		// send the second replica of the previous
		for (int j = 1; j < nReplica; j++) {
			int posPrev = (posNew - j) % nServersMax;
			if (posPrev < 0) {
				posPrev = posPrev + nServersMax;
			}
			if (posLocal == posPrev) {
				LOGGER.fine("replica: " + posNext + " address: " + address);
				Set<String> hashMap = DHTTables.get(posLocal).keySet();
				for (Iterator<String> iterator = hashMap.iterator(); iterator.hasNext();) {
					String key = (String) iterator.next();
					LOGGER.fine("posLocal + " + posLocal + " key: " + key);
					// TODO
					// sendMessages.sendPut(address, new DHT_Map(key, DHTTables.get(posLocal).get(key)), true);
				}
			}
		}
	}
	
	public boolean isQuorum() {
		return isQuorum;
	}

	private boolean isLeader() {
		try {
			List<String> list = zk.getChildren(rootMembers, false);
			Collections.sort(list);
			int index = list.indexOf(myId.substring(myId.lastIndexOf('/') + 1));
			if (index == 0) {
				System.out.println("I am the leader: " + myId);
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			System.out.println("Exception: selectLeaderWatcher");
		}
		return false;
	}
		
	public void putDHTServers(HashMap<Integer, String> newDHTServers, HashMap<Integer, DHTUserInterface> newDHTTables) {
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();
		for (int i = 0; i < nServersMax; i++) {
			if (newDHTServers.get(i) != null) {
				DHTServers.put(i, newDHTServers.get(i));
				DHTTables.put(i, newDHTTables.get(i));
				this.nServers++;
			}
		}
		LOGGER.fine(tableManager.printDHTServers());
	}

	
	

	
	

	private void configOps() {
		if (zk != null) {
			// Create a folder for members and include this process/server
			try {
				// Create a folder, if it is not created
				String response = new String();
				Stat s = zk.exists(rootOperations, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					LOGGER.fine("Creating /operations from zkMember");
					response = zk.create(rootOperations, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}

				s = zk.exists(rootOperations, false);
				List<String> list = zk.getChildren(rootOperations, watcherOperation, s);
				System.out.println("Operations in Zookeeper Cluster # operations:" + list.size());
				printListMembers(list);
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}
		}
		
	}
	
	private void configLock() {
		if (zk != null) {
			// Create a folder for locknode and include this process/server
			try {
				Stat s = zk.exists(lockPath, false);
				if (s == null) {
					int data = 0;
					byte[] d = ByteBuffer.allocate(4).putInt(data).array();
					// Created the znode, if it is not created.
					zk.create(lockPath, d, 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				// Set watcher on lockpath
				List<String> list = zk.getChildren(lockPath, false, s);
				System.out.println("Locks in Zookeeper Cluster # Locks:" + list.size());
				printListMembers(list);
				
				// Create a znode for registering as member and get my id
				// Deberia activar el watcher creado anteriormente
				lockId = zk.create(lockPath + guidLock, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				lockId = lockId.replace(lockPath + "/", "");
				System.out.println("Created znode lock id:"+ lockId );
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				System.out.println("Exception: "+ e);
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
				System.out.println("Exception: "+ e);
			}
		}
		
	}

	// Notified when the number of children in /operations is updated
	private Watcher watcherOperation = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------------------------Watcher Operation------------------------------------\n");
			try {
				List<String> list = zk.getChildren(rootOperations, watcherOperation); // this);
				System.out.println("Operations in Zookeeper Cluster # Operations:" + list.size());
				printListMembers(list);
				configLock();
				operationLeader();
			} catch (Exception e) {
				System.out.println("Exception: watcherOperation");
			}
		}
	};
	
	private boolean operationLeader() {
		System.out.println("------------------IS LOCK LEADER?------------------\n");
		try {
			List<String> list = zk.getChildren(lockPath,  false);
			Collections.sort(list);
			int index = list.indexOf(lockId.substring(lockId.lastIndexOf('/') + 1));
			String leader = list.get(0);
			leaderPath = lockPath + "/" + leader;
			if(index == 0) {
				//Es el lider
				System.out.println("[Process: " + lockId + "] I AM THE LEADER, I GET THE zOp");
				//SI ES EL LIDER DEL /LOCK, HACEMOS LA OPERACION, MODIFICAMOS EL VALOR DEL zOp Y BORRAMOS EL NODO 
				// para que otro server pueda modificar la zOp
				
				doOperation();
				
				//Borramos el nodo Lock que ha actualizado el valor del counter. 
				// Al borrar el nodo saltará un watcher al resto de clientes
				Stat s = zk.exists(leaderPath, false);
				zk.delete(leaderPath, s.getVersion());
				return true;
			} else {
				//NO ES EL LIDER
				System.out.println("[Process: " + lockId + "] - I AM NO THE LEADER! - Setting watch on node with path: " + leaderPath);				 
				System.out.println("The leader is: " + leader);
				//Ponemos watcher solo en el lider
				Stat s = zk.exists(leaderPath, watcherLock);
				// Comprobamos si existe el lider
				if (s==null) {
					// No existe, vuelvo a realizar la eleccion de lider
					operationLeader();
				}else {
					// Si existe, me quedo esperando a notificacion (llega con un watcher) y realizo la eleccion del lider
					try {
						synchronized (mutex) {
							mutex.wait();
							operationLeader();
						}
					} catch (Exception e) {
						System.out.println("Exception: " + e);
					}
				}
				return false;
			}
		} catch (Exception e) {
			System.out.println("Exception: select LockLeader");
			System.out.println("Exception: "+ e);
			return false;
		}
	}
	
	private boolean doOperation() {
		try {
			List<String> list = zk.getChildren(rootOperations, false);
			Collections.sort(list);
			String leader = list.get(0);
			String operationPath = rootOperations + "/" + leader;
			
			Stat s = zk.exists(operationPath, false);
			byte[] data = zk.getData(operationPath, false, s);
			zOpData deserializedData = DataSerialization.deserialize(data);
			int[] nodes = deserializedData.getNodes();
			boolean nodeMustDoOperation = false;
			for(int i = 0; i<nodes.length;i++) {
				if(nodes[i]== position) {
					nodeMustDoOperation = true;
				}
			}
			if(nodeMustDoOperation) {
				Operations operation = deserializedData.getOperation();
				DHT_Map map = operation.getMap();
				int value = dht.putMsg(map);
				operation.setValue(value);
				int[] answer = deserializedData.getAnswer();
				// Metemos el valor en el primer valor vacio de answer
				for(int i = 0; i< answer.length; i++) {
					if(answer[i] == 0)
						answer[i] = value; 
				}
				// Actualizamos el valor de la operacion y la respuesta
				deserializedData.setOperation(operation);
				deserializedData.setAnswer(answer);
				// Actualizamos el nodo zOp, lo que provocara un watcher en zkOp
				byte[] updatedData = DataSerialization.serialize(deserializedData);
				s = zk.exists(operationPath, false);
				zk.setData(operationPath, updatedData, s.getVersion());
			}
			
		} catch (Exception e) {
			System.out.println("Exception: selectLeaderWatcher");
		}
		return false;
	}
			
	// Notified when the number of children in /locknode is updated
	private Watcher  watcherLock = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------Watcher Lock------------------\n");		
			try {
				System.out.println("        Update!!");
				
				// Al recibir el watcher de cualquier nodo notifico a mi hebra de que levante el bloqueo
				synchronized (mutex) {
					mutex.notify();
				}
				List<String> list = zk.getChildren(lockPath,  false);
				System.out.println("Locks in Zookeeper Cluster # Locks:" + list.size());
				printListMembers(list);
			} catch (Exception e) {
				System.out.println("Exception: wacherMember");
				System.out.println("Exception: " + e);
			}
		}
	};

}