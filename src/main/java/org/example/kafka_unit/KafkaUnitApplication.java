package org.example.kafka_unit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "org.example")
public class KafkaUnitApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaUnitApplication.class, args);
	}

}
