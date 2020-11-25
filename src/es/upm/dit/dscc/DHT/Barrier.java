package es.upm.dit.dscc.DHT;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Barrier implements Watcher {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;

	private int size;
	private String name;
	String root;
	int nWatchers;
	private ZooKeeper zk;

	Integer mutexBarrier = -1;


	/**
	 * Barrier constructor
	 * 
	 * @param address
	 * @param root
	 * @param size
	 */


	public Barrier(ZooKeeper zk, int size, String root) {

		if(zk == null){
			LOGGER.severe("No ZooKeeper was found to use the Barrier");
			
		}
		//else mutex = new Integer(-1);
		this.zk = zk;
		this.root = root;
		this.size = size;

		// Create barrier node
		if (zk != null) {
			try {
				Stat s = zk.exists(root, false);
				if (s == null) {
					zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				}
			} catch (KeeperException e) {
				System.out
				.println("Keeper exception when instantiating queue: "
						+ e.toString());
			} catch (InterruptedException e) {
				System.out.println("Interrupted exception");
			}
		}

		// My node name
		try {
			//hostname
			name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
		} catch (UnknownHostException e) {
			System.out.println(e.toString());
		}
	}


	public void process(WatchedEvent event) {
		nWatchers++;
		System.out.println(">>> Process: " + event.toString() + ", " + nWatchers);
		System.out.println("Process: " + event.getType());
		synchronized (mutexBarrier) {
			mutexBarrier.notify();
		}
	}


	/**
	 * Join barrier
	 *
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */

	boolean enter() throws KeeperException, InterruptedException{
		zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		zk.exists(root + "/" + name, this);
		System.out.println("Entered barrier: " + root + " " + size);
		while (true) {
			List<String> list = zk.getChildren(root, true);

			if (list.size() < size) {
				synchronized (mutexBarrier) {
					mutexBarrier.wait();
				}
			} else {
				return true;
			}
		}
	}

	/**
	 * Wait until all reach barrier
	 *
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */

	boolean leave() throws KeeperException, InterruptedException{
		zk.delete(root + "/" + name, 0);
		while (true) {
			List<String> list = zk.getChildren(root, true);
			if (list.size() > 0) {
				synchronized (mutexBarrier) {
					mutexBarrier.wait();
				}
			} else {
				return true;
			}
		}
	}

	public static void main(String args[]) {
		System.out.println(args.length);
		System.out.println(args[0] + " " + args[1]);
		System.out.println("Entered barrier: " + args[1]);
		Barrier b = new Barrier(args[0], "/b1", new Integer(args[1]));
		try{
			boolean flag = b.enter();
			System.out.println("Entered barrier: " + args[1]);
			if(!flag) System.out.println("Error when entering the barrier");
		} catch (KeeperException e){

		} catch (InterruptedException e){

		}

		// Generate random integer
		Random rand = new Random();
		int r = rand.nextInt(100);
		// Loop for rand iterations
		for (int i = 0; i < r; i++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {

			}
		}
		try{
			b.leave();
		} catch (KeeperException e){

		} catch (InterruptedException e){

		}
		System.out.println("Left barrier");
	}
}