package com.avikdigidev.kafkabasics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;

import java.util.*;

@SpringBootApplication
public class ProducerWithCallbackApplication {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallbackApplication.class.getSimpleName());

    public static void main(String[] args) {
        //create producer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "caring-badger-9697-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Y2FyaW5nLWJhZGdlci05Njk3JFGlsZ8NndjIS8wvUckpApFsxnbEoOsRjr0rD_Y\" password=\"MGYwNTJkYjgtNTEyOC00ZjliLWI0Y2EtYjk5N2MwNmVjZTA0\";");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("kafka_topic_poc", "hey");


        //send data
        //callback happens upon successful addition of message to topic
        producer.send(producerRecord, (recordMetadata, e) -> {
            log.info("partition : "+ recordMetadata.partition());
            log.info("topic : "+ recordMetadata.topic());
            log.info("offset : "+ recordMetadata.offset());
;
        });
        //flush and close the producer
        producer.flush();
        producer.close();
    //    SpringApplication.run(ProducerWithCallbackApplication.class, args);
    }

}
