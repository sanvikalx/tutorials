package org.example.kafka_unit.produser;

import lombok.extern.slf4j.Slf4j;
import org.example.kafka_unit.dto.MessageDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public class Sender {

    @Autowired
    private KafkaTemplate<String, MessageDTO> kafkaTemplate;

    public void send(MessageDTO payload, String topicName) {
        log.info("sending payload='{}'", payload);
        kafkaTemplate.send(topicName, payload);
    }
}
