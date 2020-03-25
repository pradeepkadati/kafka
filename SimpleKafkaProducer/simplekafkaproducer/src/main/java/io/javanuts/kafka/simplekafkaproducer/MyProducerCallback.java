package io.javanuts.kafka.simplekafkaproducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducerCallback implements Callback {

	@Override
	public void onCompletion(RecordMetadata recordMetadata, Exception e) {
		if (e != null)
			System.out.println("AsynchronousProducer failed with an exception");
		else
			System.out.println("AsynchronousProducer call Success:");
	}

}
