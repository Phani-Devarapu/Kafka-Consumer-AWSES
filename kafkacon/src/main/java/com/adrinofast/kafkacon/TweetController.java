package com.adrinofast.kafkacon;

import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TweetController {
	
	static final  String INDEX = "my-index-tweet";
	   static final  String TYPE = "_doc";
	
	
	@Autowired
	private TweetService service;
	
	
	@Autowired
	private KafkaConsumerTwer kafkaConsu;



    @PutMapping("/tweets")
    public ResponseEntity createProfile(
        @RequestBody TweetDocument document) throws Exception {

        return 
            new ResponseEntity(service.createProfile(document), HttpStatus.CREATED);
    }
    
    @GetMapping("/")
    public void startTheConsumer() throws Exception {
    	
    	  String bootStrapServers = "127.0.0.1:9092";
	        String groupId = "consumer-app";

	        Properties properties = new Properties();
	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
	        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

	        KafkaConsumer<String,String> consumer = new KafkaConsumer(properties);
	        consumer.subscribe(Collections.singleton("twitter_tweets"));

	        while(true){
	           ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
	           service.createIndexRequest(records);

	        }

        
    }

}
