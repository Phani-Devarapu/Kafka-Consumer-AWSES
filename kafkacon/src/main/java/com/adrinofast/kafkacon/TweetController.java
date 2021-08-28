package com.adrinofast.kafkacon;

import org.springframework.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TweetController {
	
	private TweetService service;

    @Autowired
    public TweetController(TweetService service) {

        this.service = service;
    }

    @PutMapping("/tweets")
    public ResponseEntity createProfile(
        @RequestBody TweetDocument document) throws Exception {

        return 
            new ResponseEntity(service.createProfile(document), HttpStatus.CREATED);
    }

}
