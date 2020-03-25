package io.javanuts.kafka.simplekafkaproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * producer to send records with strings containing sequential numbers as the key/value pairs
 * Fire and Forget
 *
 */
public class App {
	public static void main(String[] args) {
		
		String topicName = "SimpleProducerTopic";

		Properties props = new Properties();
		props.put("bootstrap.servers", "10.0.0.24:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++) {
		     producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
		}

		producer.close();
		System.out.println("SimpleProducer Completed.");
	}
}
