package org.example.rest;

import lombok.RequiredArgsConstructor;
import org.example.kafka_unit.dto.MessageDTO;
import org.example.kafka_unit.produser.Sender;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class HelloController {

    private final Sender sender;

    @PostMapping("/write_to_topic/{topic_name}")
    public String writeToTopic(@PathVariable("topic_name") String topicName, MessageDTO messageDTO) {
        sender.send(messageDTO, topicName);

        return "ok";
    }

    @GetMapping("/hello")
    public String hello() {
        return  "hello";
    }


}
