package es.upm.dit.dscc.DHT;

import java.io.Serializable;
//import java.util.Set;
import java.util.HashMap;

public class Operations implements Serializable {

	private static final long serialVersionUID = 1L;
	private OperationEnum operation;
	private Integer     value         = null;       
	private String      key           = null;
	private DHT_Map     map           = null;
	private boolean     status        = false;
	private boolean     isReplica     = false;
	private int         posReplica;
	private int         posServer;
	private DHTUserInterface dht      = null;
	private HashMap<Integer, String> DHTServers;
	// private Set<String> 
	// private ArrayList<Integer>

	// PUT_MAP
	public Operations (OperationEnum operation,
			DHT_Map map){
		this.operation = operation;
		this.map       = map;
	}

	// GET_MAP REMOVE_MAP CONTAINS_KEY_MAP
	public Operations (OperationEnum operation,
			String key,           
			boolean isReplica) {
		this.operation = operation;
		this.key       = key;
		this.isReplica = isReplica;
	}

	// KEY_SET_HM, VALUES_HM, INIT	
	public Operations (OperationEnum operation) {
		this.operation = operation;
	}

	//RETURN_VALUE
	public Operations (OperationEnum operation,
			Integer value)           {
		this.operation = operation;
		this.value     = value;
	}

	//RETURN_STATUS
	public Operations (OperationEnum operation,
			boolean status)           {
		this.operation  = operation;
		this.status     = status;
	}

	//DATA_REPLICA
	public Operations ( OperationEnum operation, 
			DHTUserInterface dht, int posReplica, int posServer) {
		this.operation   = operation;
		this.dht         = dht;
		this.posReplica  = posReplica;
		this.posServer   = posServer;
	}

	//DHT_REPLICA
	public Operations ( OperationEnum operation, 
			HashMap<Integer, String> DHTServers) {
		this.operation   = operation;
		this.DHTServers  = DHTServers;
	}
	
	public OperationEnum getOperation() {
		return operation;
	}

	public void setOperation(OperationEnum operation) {
		this.operation = operation;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public DHT_Map getMap() {
		return map;
	}

	public void setMap(DHT_Map map) {
		this.map = map;
	}

	public boolean getStatus() {
		return status;
	}

	public void setMap(boolean status) {
		this.status = status;
	}

	public boolean isReplica() {
		return isReplica;
	}

	public void setReplica(boolean isReplica) {
		this.isReplica = isReplica;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public DHTUserInterface getDHT() {
		return this.dht;
	}
	
	public int getPosReplica() {
		return this.posReplica;
	}
	
	public HashMap<Integer, String> getDHTServers() {
		return this.DHTServers;
	}

	// LIST_SERVERS
	// Es posible que no sea necesario
	
	
}

