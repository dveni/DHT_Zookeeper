package es.upm.dit.dscc.DHT;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class zOpData implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	private Operations operation;
	private int[] nodes;
	
	public zOpData(Operations operation, int[] nodes) {
		super();
		this.operation = operation;
		this.nodes = nodes;
	}

	public Operations getOperation() {
		return operation;
	}

	
	public int[] getNodes() {
		return nodes;
	}

	

	@Override
	public String toString() {
		return "zOpData [operation=" + operation + ", nodes=" + Arrays.toString(nodes) + "]";
	}

	
	
	
	
}
