package io.javanuts.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName()); 
	private final String consumerKey = "HSv3cTbgC2buPk2qcIut6RJKm";
	private final String consumerSecret = "Wf0VeeWuKqvWg9LDC5hSRTc1LuVgId8rbhSXry3xbaKdl8i19H";
	private final String token = "752184488931659776-ZFduH7IeUBUXUQZtnH7mgFV22FiBz6d";
	private final String secret = "yMI0uiMOCe0n6KSsnIJFlKxa3olbqlhfiu50QTUTfH6O3";

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	public void run() {
		
	 // Create Twitter Client
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		Client client =  createTwitterClient(msgQueue);
		
	// Establish Twitter connection
		client.connect();
		
    // Create Kafka Producer
		
		KafkaProducer<String, String> producer = creatKafkaProducer();
		
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("Closing application...");
			
			logger.info("Closing Twitter Clinet...");
			client.stop();
			
			logger.info("Closing Producer...");
			producer.close();
			
			logger.info("Done...");
			
			
		}));
		
	// Check the Twitter data
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
		  try {
			 msg = msgQueue.poll(5,TimeUnit.SECONDS );
			 if (msg != null) {
				 logger.info(msg);	
				 producer.send(new ProducerRecord<String, String>("twitter_tweets", null,msg));
			 }
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			client.stop();
		}finally {
			
		}
		  
		}
		
		

	}

	private KafkaProducer<String, String> creatKafkaProducer() {
		 Properties properties = new Properties();
	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
	        
	        // Creating Safe Producer
	        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
	        
	        // by default the below properties will be set to producer when idempotence is enabled
	        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
	        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE) );
	        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
	        
	        // Create high throughput producer at the cost of some latency and CPU cycles
	        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
	        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
	        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
	        
	        // create kafka producer
	        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms

		List<String> terms = Lists.newArrayList("kafka","ipl","india");

		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}

}
