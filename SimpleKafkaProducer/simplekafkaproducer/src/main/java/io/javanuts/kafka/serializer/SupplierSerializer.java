package io.javanuts.kafka.serializer;

import java.nio.ByteBuffer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class SupplierSerializer implements Serializer<Supplier> {
	
	private String encoding = "UTF8";
	
	@Override
	public byte[] serialize(String topic, Supplier data) {
		int sizeOfName;
        int sizeOfDate;
        byte[] serializedName;
        byte[] serializedDate;
                                
        try {
                if (data == null)
                    return null;
            
                serializedName = data.getName().getBytes(encoding);
                sizeOfName = serializedName.length;
                serializedDate = data.getStartDate().toString().getBytes(encoding);
                sizeOfDate = serializedDate.length;

                ByteBuffer buf = ByteBuffer.allocate(4+4+sizeOfName+4+sizeOfDate);
                buf.putInt(data.getID());
                buf.putInt(sizeOfName);
                buf.put(serializedName);
                buf.putInt(sizeOfDate);
                buf.put(serializedDate);                                    

                return buf.array();

            } catch (Exception e) {
                throw new SerializationException("Error when serializing Supplier to byte[]");
        }
	}

}
