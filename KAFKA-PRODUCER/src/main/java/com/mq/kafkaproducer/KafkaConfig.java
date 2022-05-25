package com.mq.kafkaproducer;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${kafka.host}")
    private String host;

    @Value("${kafka.username}")
    String username;

    @Value("${kafka.password}")
    String password;

    @Value("${kafka.topic}")
    private String topic;


    @Value("${kafka.group-id}")
    String groupId;

    private Map<String, Object> producerConfigs() {
        final Map<String, Object> hashMap = new HashMap<>();
//        hashMap.put("security.protocol", "PLAINTEXT");
//
//        hashMap.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//        hashMap.put(SaslConfigs.SASL_JAAS_CONFIG,
//                    String.format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
//                                          "      username=\"%s\" " +
//                                          "      password=\"%s\";", username, password));
        hashMap.put(ProducerConfig.ACKS_CONFIG, "all");
        hashMap.put("enable.partition.eof", "false");
        hashMap.put("group.id", groupId);
        hashMap.put(ProducerConfig.RETRIES_CONFIG, 3);
        hashMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        hashMap.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        hashMap.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        hashMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        hashMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return hashMap;
    }


    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

//    @Bean
//    public KafkaAdmin kafkaAdmin() {
//        Map<String, Object> configs = new HashMap<>();
//        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, host);
//        return new KafkaAdmin(configs);
//    }

    @Bean
    public NewTopic topic1() {

         return new NewTopic(topic, 1, (short) 1);
    }
}
