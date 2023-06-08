package com.usuarios.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.usuarios.data.Datos;
import com.usuarios.producer.Productor;

import lombok.extern.slf4j.Slf4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class Usercontroller {

    @Autowired
    Productor EventProducer;
    
    private static final Logger LOG = LoggerFactory.getLogger("Usercontroller");
    String SendResult;
    
    
    @PostMapping("/envio")
    public ResponseEntity<Datos> postrestusers(@RequestBody Datos usuarios) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        //invoke kafka producer
        LOG.info("before sendLibraryEvent");
        //libraryEventProducer.sendLibraryEvent(libraryEvent);
        SendResult = EventProducer.ejecutar(usuarios);
        LOG.info("SendResult is {} ", SendResult);
        LOG.info("after sendLibraryEvent");
        return ResponseEntity.status(HttpStatus.CREATED).body(usuarios);
    }
}
