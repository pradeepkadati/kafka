package io.javanuts.kafka.elastic.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

;

public class ElasticSearchConsumer {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
	private static JsonParser parser = new JsonParser();

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String topicName = "twitter_tweets";
		RestHighLevelClient client = createRestHighLevelClient();
		String jsonString = "{\"foo\":\"bar\" }";
		//IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);

		
		
		KafkaConsumer<String, String> consumer = createConsumer(topicName);
		
		 while(true){
	            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
	            
	            int recordCount = records.count();
	            
	            logger.info("Records fetched " +recordCount + " records" );
	            
	            BulkRequest bulkRequest = new BulkRequest();
	            
	            for (ConsumerRecord<String,String> record:records ){
	            	
	            	String id = extractIdFromTweet(record.value());
	                
	            	IndexRequest indexRequest = new IndexRequest("twitter", "tweets",id).source(record.value(), XContentType.JSON);	
	            	bulkRequest.add(indexRequest);
	            	//IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
	        		//logger.info(response.getId());
	        		try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            }
	            if (recordCount > 0) {
	            	BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		            
		            logger.info("Committing the Offsets..." );
		            consumer.commitAsync();
		            logger.info("Offsets Committed" );
	            }
	            
	        }


	}

	private static String extractIdFromTweet(String value) {
		
		return parser.parse(value).getAsJsonObject().get("id_str").getAsString();
	}

	public static KafkaConsumer<String, String> createConsumer(String topic) {

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-twitter-elasticsearch");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

		// Create Consumer

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		consumer.subscribe(Arrays.asList(topic));
		
		return consumer;
	}

	public static RestHighLevelClient createRestHighLevelClient() {
		
		String hostName = "hostname";
		String userName = "username";
		String password = "password";

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	
	

}
