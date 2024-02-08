package com.avikdigidev.kafkabasics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaBasicsApplication {


    public static void main(String[] args) {
        System.out.println("hey");
        SpringApplication.run(KafkaBasicsApplication.class, args);
    }

}
