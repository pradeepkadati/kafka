package io.javanuts.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger logger =  LoggerFactory.getLogger(ProducerDemoWithKeys.class);
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
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata rmd, Exception e) {
                if (e == null){
                    logger.info("Received New Metadata \n" +
                            "Topic : "+rmd.topic() +"\n"+
                            "Offset: "+rmd.offset() + "\n"+
                            "Partition: "+rmd.partition()+ "\n"+
                            "TimeStamp: "+rmd.timestamp());
                }else{
                    logger.error("error while publishing record ",e);
                }
            }
        });

        producer.flush();
        producer.close();
    }
}
