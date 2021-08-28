package com.adrinofast.kafkacon;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;


@Service
public class TweetService {
	
	final  String INDEX = "my-index-tweet";
	   final  String TYPE = "_doc";
	
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
//
//	        Map<String, String> documentMapper = new HashMap<>();
//	        documentMapper.put("hello", "ia m good");
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

}
