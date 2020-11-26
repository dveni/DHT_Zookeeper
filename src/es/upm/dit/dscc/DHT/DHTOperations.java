package es.upm.dit.dscc.DHT;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
//import java.util.Collection;
//import java.util.Iterator;
import java.util.Set;

import java.util.logging.ConsoleHandler;
import java.util.logging.Filter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.zookeeper.data.Stat;

public class DHTOperations implements DHTUserInterface {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
   
	private operationBlocking mutex;
	private TableManager      tableManager;
	private int nReplica;

	public DHTOperations (operationBlocking mutex, TableManager tableManager, int nReplica) {
		this.mutex        = mutex;
		this.tableManager = tableManager;
		this.nReplica = nReplica;
		
	}

	
	@Override
	public Integer putMsg(DHT_Map map) {
		
		return putLocal(map);
	}
	
	@Override
	public Integer put(DHT_Map map) {
		LOGGER.finest("PUT: Is invoked");
		Operations operation = new Operations(OperationEnum.PUT_MAP, map); 	
		// Create the array of nodes where map should be stored
		int nodes[] = tableManager.getNodes(map.getKey());
		
		zOpData opData = new zOpData(operation, nodes, nReplica);
		
		//Serializar datos de operacion (put map) y nodos que deben hacerla 
		byte[] data = DataSerialization.serialize(opData);
		
		// Creamos zNode con operacion y sus datos 
		zkOperation op = new zkOperation(data, mutex);
		
		//Cuando se borre la operacion porque ya ha terminado de ejecutarse, 
		// deberá saltar un watcher que notifique a este mutex para responder al cliente
		LOGGER.finest("Entremos en mutex.sendOperation()");
		operation = mutex.sendOperation();
		LOGGER.finest("Returned value in put: " + operation.getValue());
		return operation.getValue();
		
	}
	

	private Integer putLocal(DHT_Map map) {
		DHTUserInterface  hashMap;
		hashMap = tableManager.getDHT(map.getKey());
		
		if (hashMap == null) {
			LOGGER.warning("Error: this sentence should not get here");
		}		
		return hashMap.put(map);
	}


	@Override
	public Integer get(String key) {

		LOGGER.finest("GET: Is invoked");
		Operations operation = new Operations(OperationEnum.GET_MAP, key); 	
		// Create the array of nodes where map should be stored
		int nodes[] = tableManager.getNodes(key);
		
		zOpData opData = new zOpData(operation, nodes, nReplica);
		
		//Serializar datos de operacion (put map) y nodos que deben hacerla 
		byte[] data = DataSerialization.serialize(opData);
		
		// Creamos zNode con operacion y sus datos 
		zkOperation op = new zkOperation(data, mutex);
		
		//Cuando se borre la operacion porque ya ha terminado de ejecutarse, 
		// deberá saltar un watcher que notifique a este mutex para responder al cliente
		LOGGER.finest("Entremos en mutex.sendOperation()");
		operation = mutex.sendOperation();
		LOGGER.finest("Returned value in put: " + operation.getValue());
		return operation.getValue();
	}
	@Override
	public Integer getMsg(String key) {
		
		return getLocal(key);
	}
	private Integer getLocal(String key) {
		DHTUserInterface  hashMap;
		hashMap = tableManager.getDHT(key);
		
		if (hashMap == null) {
			LOGGER.warning("Error: this sentence should not get here");
		}
		
		return hashMap.get(key);		
	}
	
	public Integer removeMsg(String key) {
		return removeLocal(key);
	}
	
	@Override
	public Integer remove(String key) {

		LOGGER.finest("REMOVE: Is invoked");
		Operations operation = new Operations(OperationEnum.REMOVE_MAP, key); 	
		// Create the array of nodes where map should be stored
		int nodes[] = tableManager.getNodes(key);
		
		zOpData opData = new zOpData(operation, nodes, nReplica);
		
		//Serializar datos de operacion (put map) y nodos que deben hacerla 
		byte[] data = DataSerialization.serialize(opData);
		
		// Creamos zNode con operacion y sus datos 
		zkOperation op = new zkOperation(data, mutex);
		
		//Cuando se borre la operacion porque ya ha terminado de ejecutarse, 
		// deberá saltar un watcher que notifique a este mutex para responder al cliente
		LOGGER.finest("Entremos en mutex.sendOperation()");
		operation = mutex.sendOperation();
		LOGGER.finest("Returned value in put: " + operation.getValue());
		return operation.getValue();


	}

	private Integer removeLocal(String key) {
		DHTUserInterface  hashMap;
		hashMap = tableManager.getDHT(key);
		
		if (hashMap == null) {
			LOGGER.warning("Error: this sentence should not get here");
		}
		
		return hashMap.remove(key);		
	}
	
	@Override
	public boolean containsKey(String key) {
		Integer isContained = get(key);
		if (isContained == null) {
			return false;
		} else {
			return true;
		}
	}
	
	@Override
	public Set<String> keySet() {
		// Notify the operation to the cluster

		// Update the operation
		return null; //hashMap.keySet();
	}

	@Override
	public ArrayList<Integer> values() {
		// Notify the operation to the cluster

		// Update the operation
		return null;//hashMap.values();

	}

	@Override
	public String toString() {
		
		return tableManager.toString();

	}




	
	
		
	

}
