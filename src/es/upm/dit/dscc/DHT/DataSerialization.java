package es.upm.dit.dscc.DHT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class DataSerialization {

	private static java.util.logging.Logger LOGGER = DHTMain.LOGGER;

	public DataSerialization() {
		super();
	}
	public static byte[] serialize(zOpData data) {
		byte[] serializedData = null;
		
		LOGGER.fine("Serializing data: " + data.toString());
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(bos);
			oos.writeObject(data);
			oos.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		serializedData = bos.toByteArray();
		
		return serializedData;
		
	}
	
	public static zOpData deserialize(byte[] data) {
		LOGGER.fine("Deserializing data: " + data.toString());
		zOpData operation = null;
		// Deserialize: Convert an array of Bytes in an operation.
		if (data != null) {
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(data);
				ObjectInputStream     in = new ObjectInputStream(bis);
				operation  = (zOpData) in.readObject();
			}
			catch (Exception e) {
				System.out.println("Error while deserializing object");
			}

		}
		return operation;

	}

	
	
}

