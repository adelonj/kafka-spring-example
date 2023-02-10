package com.example.kafka.event.producer;

import com.example.kafka.dto.MessageDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
@Log4j2
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value(value = "${kafka.topic.name.message-send}")
    private String topic;

    public void send(MessageDTO messageDTO) {
        if(ObjectUtils.isEmpty(messageDTO.getRqUid()))
            messageDTO.setRqUid(UUID.randomUUID().toString());

        if(ObjectUtils.isEmpty(messageDTO.getDateTime()))
            messageDTO.setDateTime(LocalDateTime.now());

        ObjectMapper om = new ObjectMapper();
        try {
        String payload = om.writeValueAsString(messageDTO);
            log.info("sending payload='{}' to topic='{}'", payload, topic);
            kafkaTemplate.send(topic, payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}