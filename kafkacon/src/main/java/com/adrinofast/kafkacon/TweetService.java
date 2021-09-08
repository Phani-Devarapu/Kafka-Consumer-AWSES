package com.adrinofast.kafkacon;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;


@Service
public class TweetService {
	
	final  String INDEX = "my-index-tweet";
	   final  String TYPE = "_doc";
	
	   @Autowired
	   private RestHighLevelClient client;

       private ObjectMapper objectMapper;

	    @Autowired
	    public TweetService(RestHighLevelClient client, ObjectMapper objectMapper) {
	        this.client = client;
	        this.objectMapper = objectMapper;
	    }

	    public String createProfile(TweetDocument document) throws Exception {

	        UUID uuid = UUID.randomUUID();
	        document.setId(uuid.toString());

	        TweetDocument tw = new TweetDocument();
	        tw.setBy("ffd");
	        tw.setId("plol");
	        tw.setMessage("hellpp kafka");
	        Map<String, Object> documentMapper = objectMapper.convertValue(tw, Map.class);;

	        IndexRequest indexRequest = new IndexRequest(INDEX).id("3").source(documentMapper);
	               

	        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
	        System.out.println(indexResponse);

	        return indexResponse
	                .getResult()
	                .name();
	    }
	    
	    public void createIndexRequest(ConsumerRecords<String, String> records) {
	    	
	   	records.forEach(record->{
	   		System.out.println(record.key());
	   		System.out.println(record.value());
	   		
	    		IndexRequest indexRequest = new IndexRequest(INDEX).id(record.key()).source(record.value(),XContentType.JSON);
	    		 try {
					IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
					System.out.println(indexResponse.getIndex());
					
				} catch (IOException e) {
					e.printStackTrace();
				}	
	    	});
	    	
	    	
	    }
}
