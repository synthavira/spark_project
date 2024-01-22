package com.spark.FileTransfer.Config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.context.annotation.Bean;

@Configuration
public class FileTransferTopic {
	
	  @Bean
	    public NewTopic myTopic() {
	        return TopicBuilder.name("file_metadata")
	                .partitions(4)
	                .replicas(1)
	                .build();
	    }
	
	  @Bean
	  public KafkaAdmin kafkaAdmin() {
	        
	        Map<String, Object> configs = new HashMap<>();
	        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000); 
	        return new KafkaAdmin(configs);
	    }

}
