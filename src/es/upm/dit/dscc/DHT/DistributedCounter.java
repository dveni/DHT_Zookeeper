package es.upm.dit.dscc.DHT;

import java.util.Iterator;
import java.util.List;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;

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

public class DistributedCounter implements Watcher{
	private static ZooKeeper zk;
	private static final int SESSION_TIMEOUT = 5000;

	//Elemento raiz
	private static String rootMembers = "/members";
	//Miembros
	private static String aMember = "/member-";
	
	private String myId;

	private static String leaderPath;

	//LOCK
	private static String lockPath = "/locknode";
	private static String guidLock = "/guid-lock-";
	
	//CONTADOR
	private static int counter = 0;
	private static Integer mutex = -1;

	
	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

	public DistributedCounter () {

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
					System.out.println("Exception: " + e);
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
				Stat s = zk.exists(rootMembers, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					zk.create(rootMembers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}			
				// Create a znode for registering as member and get my id
				myId = zk.create(rootMembers + aMember, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				myId = myId.replace(rootMembers + "/", "");
				List<String> list = zk.getChildren(rootMembers, false, s); //this, s);
				System.out.println("Created znode nember id:"+ myId );
				printListMembers(list);
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
			System.out.println(e.toString());
			notify();
		}
	};

	// Notified when the number of children in /locknode is updated
	private Watcher  watcherMember = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------Watcher Locker------------------\n");		
			try {
				System.out.println("        Update!!");
				
				// Al recibir el watcher de cualquier nodo notifico a mi hebra de que levante el bloqueo
				synchronized (mutex) {
					mutex.notify();
				}
				List<String> list = zk.getChildren(lockPath,  false); //this);
				printListMembers(list);
			} catch (Exception e) {
				System.out.println("Exception: wacherMember");
				System.out.println("Exception: " + e);
			}
		}
	};
		
	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
			List<String> list = zk.getChildren(lockPath, watcherMember); //this);
			printListMembers(list);
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
	
	//Metodo para determinar si un znode es el lider dentro del znode LOCK
	private boolean isLeader(Integer value) {
		System.out.println("------------------IS LOCK LEADER?------------------\n");
		try {
			List<String> list = zk.getChildren(lockPath,  false);
			Collections.sort(list);
			int index = list.indexOf(myId.substring(myId.lastIndexOf('/') + 1));
			String leader = list.get(0);
			leaderPath = lockPath + "/" + leader;
			if(index == 0) {
				//Es el lider
				System.out.println("[Process: " + myId + "] I AM THE LEADER, I GET THE COUNTER");
				//SI ES EL LIDER DEL /LOCK, AUMENTAMOS CONTADOR Y BORRAMOS EL NODO
				incrementCounterValue(value);
				//Borramos el nodo Lock que ha actualizado el valor del counter. 
				// Al borrar el nodo saltará un watcher al resto de clientes
				Stat s = zk.exists(leaderPath, false);
				zk.delete(leaderPath, s.getVersion());
				return true;
			} else {
				//NO ES EL LIDER
				System.out.println("[Process: " + myId + "] - I AM NO THE LEADER! - Setting watch on node with path: " + leaderPath);				 
				System.out.println("The leader is: " + leader);
				//Ponemos watcher solo en el lider
				Stat s = zk.exists(leaderPath, watcherMember);
				// Comprobamos si existe el lider
				if (s==null) {
					// No existe, vuelvo a realizar la eleccion de lider
					isLeader(value);
				}else {
					// Si existe, me quedo esperando a notificacion (llega con un watcher) y realizo la eleccion del lider
					try {
						synchronized (mutex) {
							mutex.wait();
							isLeader(value);
						}
					} catch (Exception e) {
						System.out.println("Exception: " + e);
					}
				}
				return false;
			}
		} catch (Exception e) {
			System.out.println("Exception: select Leader");
			System.out.println("Exception: "+ e);
			return false;
		}
	}
	
	//Metodo para modificar el valor del contador (se almacena en data del Locknode)
	private void addCounterValue(Integer value) {
		if (zk != null) {
			// Create a folder for locknode and include this process/server
			try {
				Stat s = zk.exists(lockPath, false);
				if (s == null) {
					//Creamos LOCKNODE e inicializamos el valor del contador a 0
					//Data -> Counter = 0 (Valor inicial)
					int data = 0;
					byte[] d = ByteBuffer.allocate(4).putInt(data).array();
					// Created the znode, if it is not created.
					zk.create(lockPath, d, 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				// Set watcher on lockpath
				List<String> list = zk.getChildren(lockPath, false, s); //this, s);
				printListMembers(list);
				
				// Create a znode for registering as member and get my id
				// Deberia activar el watcher creado anteriormente
				myId = zk.create(lockPath + guidLock, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				myId = myId.replace(lockPath + "/", "");
				System.out.println("Created znode lock id:"+ myId );
				isLeader(value);		
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
	
	private static int getCounterValue() {
		//Obtenemos el valor del contador a partir del data almacenado en LockNode
		//Inicializamos al valor guardado en la hebra (No es el correcto)
		int data = counter;
		try {
			Stat s = zk.exists(lockPath, false);
			byte[] b = zk.getData(lockPath, false, s);
			ByteBuffer buffer = ByteBuffer.wrap(b); 
			//Ultimo valor del contador
			data = buffer.getInt();
			return data;
		} catch (Exception e) {
			System.out.println("Error: Valor del contador erroneo");
			System.out.println("Exception: " + e);
		}
		return data;
	}
	
	private void incrementCounterValue(Integer value) {
		//Actualizamos el valor del contador
		try {
			//Obtenemos el valor del contador a partir del data almacenado en LockNode
			int lastCounter = getCounterValue();
			int newCounter = lastCounter + value;
			counter = newCounter;
			//Lo convertimos en un array de bytes
			byte[] d = ByteBuffer.allocate(4).putInt(newCounter).array();
			Stat s = zk.exists(lockPath, false);
			zk.setData(lockPath, d, s.getVersion());
		} catch (Exception e) {
			System.out.println("Error: Incremento del contador erroneo");
			System.out.println("Exception: " + e);
		}
	}

	
	public static void main(String[] args) {
		DistributedCounter zk = new DistributedCounter();
		Thread thread = new Thread() {
			public void run() {
				for(int i=0; i<100; i++) {
					System.out.println("----------------------------------------------");
					System.out.println("------------------OPERACION " + i +"----------------------");
					System.out.println("Operacion de añadir al counter: "+ i);
					counter = getCounterValue();
					System.out.println("Valor actual del counter:" + counter);
					zk.addCounterValue(1);
					try {
						Thread.sleep(100);
					} catch (Exception e) {
						System.out.println("Exception: " + e);
					}
					System.out.println("----------------------------------------------");
					System.out.println("---------------FINALIZADA OPERACION " + i +"----------------");
					counter = getCounterValue();
					System.out.println("Updated counter value >> " + counter);
				}
				System.out.println("Finalizando el proceso...");
				return;
			}
		};
		thread.start();
	}
}