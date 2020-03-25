package io.javanuts.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsynchronousKafkaProducer {

	public static void main(String[] args) {
		String key = "Key1Sync";
        String value = "Value-1-Async";
        String topicName = "SimpleProducerTopic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
     
		producer.send(record,new AsyncCallback());
		
		producer.close();
	
        System.out.println("SimpleProducer Completed.");

	}
	
  

}

class AsyncCallback implements Callback{

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (exception != null)
            System.out.println("AsynchronousProducer failed with an exception");
        else
            System.out.println("AsynchronousProducer call Success:");
		
	}
	
	  
  }
