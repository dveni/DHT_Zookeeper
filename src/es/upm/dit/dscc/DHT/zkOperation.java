package es.upm.dit.dscc.DHT;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
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

public class zkOperation implements Watcher{
	
	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	
	
	private static final int SESSION_TIMEOUT = 5000;
	//Nodo raiz
	private static String rootOperations = "/operations";
	//Operaciones
	private static String aOperation = "/oper-";
	private String operationId;
	private String pathToOperation;

	
	private operationBlocking mutex;

	
	private static String leaderPath;
	
	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

	private ZooKeeper zk;
	
	public zkOperation (byte[] data, operationBlocking mutex) {
		
		this.mutex = mutex;
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
				Stat s = zk.exists(rootOperations, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootOperations, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}

				// Create a znode for registering as member and get my id
				pathToOperation = zk.create(rootOperations + aOperation, data, 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				// Cuando hace esto los watcher ya han hecho la operacion
				
				Stat opStat = zk.exists(pathToOperation, false);
				byte[] opData = zk.getData(pathToOperation, watcherData, opStat);
				zOpData reconstructedData = DataSerialization.deserialize(opData);
				
				operationId = pathToOperation.replace(rootOperations + "/", "");
				List<String> list = zk.getChildren(rootOperations, false, s); //this, s);

				System.out.println("Created znode operation id:"+ operationId );
				System.out.println("Data in zOperation node: " + reconstructedData.toString());
				printListMembers(list);
				//isLeader();
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}

		}
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
			List<String> list = zk.getChildren(rootOperations, false); //this);
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
	
	
	// Notified when the number of children in /member is updated
	private Watcher  watcherMember = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------Watcher Member------------------\n");		
			try {
							
				List<String> list = zk.getChildren(rootOperations,  false); //this);
				printListMembers(list);
				
			} catch (Exception e) {
				System.out.println("Exception: wacherMember");
			}
		}
	};
	
	// Notified when the data in a zOp node I have created is updated
	private Watcher  watcherData = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------Watcher Data------------------\n");		
			try {
				Stat opStat = zk.exists(pathToOperation, false);
				byte[] opData = zk.getData(pathToOperation, false, opStat);
				zOpData reconstructedData = DataSerialization.deserialize(opData);
				int[] answer = reconstructedData.getAnswer();
				LOGGER.fine("The operation path is: " + pathToOperation);
				LOGGER.fine("The answer array now has in the position 0: " + answer[0]);
				LOGGER.fine("The answer array now has in the position 1: " + answer[1]);
				// Por defecto se inicializa con todo ceros
				// Si la respuesta de algun server es cero, aun no ha ejecutado la operacion 
				
				boolean allServersPerformedOp = true;
				for(int i= 0; i<answer.length; i++) {
					if(answer[i]==0) {
						allServersPerformedOp = false;
						LOGGER.warning("watcherData: Server at pos" + i +" has not perfomed the operation yet");
					}
				}
				
				if(allServersPerformedOp ) {
					// Ambos servidores han ejecutado la operacion
					// Se puede meter logica de recuperacion de errores si vemos que las respuestas son distintas
					Operations operation = reconstructedData.getOperation();
					mutex.receiveOperation(operation);
					//Borramos el nodo op cuando se ha realizado
					// Al borrar el nodo saltará un watcher al resto de clientes
					Stat s = zk.exists(pathToOperation, false);
					zk.delete(pathToOperation, s.getVersion());
				}
				
			} catch (Exception e) {
				System.out.println("Exception: wacherData");
			}
		}
	};
		
	


	
	
}