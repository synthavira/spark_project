package com.spark.FileTransfer.Controller;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FileTransferController {
	
	public static void main(String[] args) {
        String kafkaBootstrapServers = "localhost:9092";
        String kafkaTopic = "file_metadata";

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "metadata-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
   
            consumer.subscribe(Collections.singletonList(kafkaTopic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    System.out.println("Received Metadata: " + record.value());
                  
                });
            }
        }
    }

}
