package org.example.kafka_unit.consumer;

import java.util.Stack;
import java.util.concurrent.CountDownLatch;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.example.kafka_unit.dto.MessageDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@Data
public class Receiver {

  private CountDownLatch latch = new CountDownLatch(1);

  private Stack<MessageDTO> stack = new Stack<>();

  public CountDownLatch getLatch() {
    return latch;
  }

  @KafkaListener(topics = "receiver.t")
  public void receive(MessageDTO payload) {
    stack.push(payload);
    log.info("received payload='{}'", payload);
    latch.countDown();
  }
}
