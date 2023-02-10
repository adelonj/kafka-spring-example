package com.example.kafka.controller;

import com.example.kafka.dto.MessageDTO;
import com.example.kafka.event.producer.KafkaProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
@RequestMapping("/message")
public class ProducerController {
    private final KafkaProducer producer;

    @PostMapping
    public ResponseEntity<String> sendMessage(@RequestBody @Valid MessageDTO message){
        producer.send(message);
        return  ResponseEntity.ok().body("Message sent successfully");
    }
}
