package com.camacuchi.kafka.valley;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafkaStreams
@SpringBootApplication
public class KafkaValleyApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaValleyApplication.class, args);
	}

}
