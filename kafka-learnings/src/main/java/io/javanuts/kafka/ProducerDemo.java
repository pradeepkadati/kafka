package io.javanuts.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
       // create kafka properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create kafka producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // create kafka record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","hello World Kafka");

        // send the kafka record
        producer.send(record);

        producer.flush();
        producer.close();
    }
}
