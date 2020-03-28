package io.javanuts.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.javanuts.kafka.serializer.Supplier;

public class SupplierConsumer {

	public static void main(String[] args) throws Exception {

        String topicName = "SupplierTopic";
        String groupName = "SupplierTopicGroup";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.javanuts.kafka.serializer.SupplierDeserializer");

        KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        while (true) {
            ConsumerRecords<String, Supplier> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Supplier> record : records) {
                System.out.println("Supplier id= " + String.valueOf(record.value().getSupplierId()) + " Supplier  Name = " + record.value().getSupplierName() + " Supplier Start Date = " + record.value().getSupplierStartDate().toString());
            }
        }

    }
	
}
