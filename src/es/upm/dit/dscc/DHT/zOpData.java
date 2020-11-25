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
	private int[] answer;
	
	public zOpData(Operations operation, int[] nodes, int nReplica) {
		super();
		this.operation = operation;
		this.nodes = nodes;
		this.answer = new int[nReplica];
	}

	public int[] getAnswer() {
		return answer;
	}

	public void setAnswer(int[] answer) {
		this.answer = answer;
	}

	public Operations getOperation() {
		return operation;
	}

	
	public void setOperation(Operations operation) {
		this.operation = operation;
	}

	public int[] getNodes() {
		return nodes;
	}

	

	@Override
	public String toString() {
		return "zOpData [operation=" + operation + ", nodes=" + Arrays.toString(nodes) + "]";
	}

	
	
	
	
}
