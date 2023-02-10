package com.example.kafka.event.consumer;

import com.example.kafka.dto.MessageDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class KafkaConsumer {

    @KafkaListener(topics = "${kafka.topic.name.message-get}", groupId = "${kafka.groupId}}", containerFactory = "kafkaListenerContainerFactory")
    public void listenGroup(MessageDTO message) throws JsonProcessingException {
        log.info("Received Message");
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(message);
        log.info("Json message received using Kafka listener " + jsonString);
    }
}
