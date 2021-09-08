package com.adrinofast.kafkacon;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Component
public class KafkaConsumerTwer {
	static final  String INDEX = "my-index-tweet";
	   static final  String TYPE = "_doc";
	   static Logger logger = LoggerFactory.getLogger(KafkaConsumerTwer.class);
	
	  
	//   private static RestHighLevelClient client  = new RestHighLevelClient(null)

	   
	 //   private ObjectMapper objectMapper;
	   
	   void sampleTest(){
		   
	   }

	
	 public static void main(String[] args) {
	       
//
//			        String bootStrapServers = "127.0.0.1:9092";
//			        String groupId = "consumer-app";
//		
//			        Properties properties = new Properties();
//			        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
//			        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//			        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//			        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//			        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
//		
//			        KafkaConsumer<String,String> consumer = new KafkaConsumer(properties);
//			        consumer.subscribe(Collections.singleton("twitter_tweets"));
//		
//			        while(true){
//			           ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
//			           
//			           records.forEach(record->{
//			        	   IndexRequest indexRequest = new IndexRequest(INDEX).id(record.key()).source(record.value(),XContentType.JSON);
//			        	   IndexResponse indexResponse;
//						try {
//							indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//							 System.out.println(indexResponse);
//						} catch (IOException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//			   	       
//		
//		//	   	        return indexResponse
//		//	   	                .getResult()
//		//	   	                .name();
//			           });
//		
//			        }

	    }
	 
	  public String createProfile(TweetDocument document) throws Exception {

//	        UUID uuid = UUID.randomUUID();
//	        document.setId(uuid.toString());
////
////	        Map<String, String> documentMapper = new HashMap<>();
////	        documentMapper.put("hello", "ia m good");
//	        TweetDocument tw = new TweetDocument();
//	        tw.setBy("ffd");
//	        tw.setId("plol");
//	        tw.setMessage("hellpp kafka");
//	        Map<String, Object> documentMapper = objectMapper.convertValue(tw, Map.class);;
//
//	        IndexRequest indexRequest = new IndexRequest(INDEX).id("3").source(documentMapper);
//	               
//
//	        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//	        System.out.println(indexResponse);
  return null;
//	        return indexResponse
//	                .getResult()
//	                .name();
	    }
	    
	    public static void createIndexRequest(ConsumerRecords<String, String> records) {
	    	
//	   	records.forEach(record->{
//	   		logger.info(record.key() + " " + record.value());
//	    		IndexRequest indexRequest = new IndexRequest(INDEX).id(record.key()).source(record.value());
//	    		 try {
//					IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//				} catch (IOException e) {
//					e.printStackTrace();
//				}	
//	    	});
	    		
	    }
}
