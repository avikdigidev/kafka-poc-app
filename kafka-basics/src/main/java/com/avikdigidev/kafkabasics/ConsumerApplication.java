package com.avikdigidev.kafkabasics;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.*;

import java.time.*;
import java.util.*;

@SpringBootApplication
public class ConsumerApplication {

    private static final Logger log = LoggerFactory.getLogger(ConsumerApplication.class.getSimpleName());

    public static void main(String[] args) {
        String groupId = "my-java-application";
        String topic = "kafka_topic_poc";
        //create producer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "caring-badger-9697-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");

        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Y2FyaW5nLWJhZGdlci05Njk3JFGlsZ8NndjIS8wvUckpApFsxnbEoOsRjr0rD_Y\" password=\"MGYwNTJkYjgtNTEyOC00ZjliLWI0Y2EtYjk5N2MwNmVjZTA0\";");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

/*none - None means that if we don't have any existing consumer group, then we fail. That means that we must set the consumer group before starting the application,
existing - Earliest means read from the beginning of my topic. This corresponds to the minus minus from beginning option
latest -  latest means to read it from just now and only read the new messages sent from now."
* */
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //subsribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            log.info("consumer polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info(record.key() + "   -   " + record.value()+ "   -   " + record.partition()+ "   -   " + record.offset());
            }
        }
        //poll for data


    }

}
