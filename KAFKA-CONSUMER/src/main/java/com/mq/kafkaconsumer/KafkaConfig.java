package com.mq.kafkaconsumer;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${kafka.host}")
    private String host;

    @Value("${kafka.username}")
    String username;

    @Value("${kafka.password}")
    String password;

    public final static String TOPIC = "kafka-topic-kmq";

    public final static String GROUP_ID = "kafka-group-id-gp";

    private Map<String, Object> consumerConfigs() {

        JsonDeserializer<MessageBody>
                deserializer = getDeserializer();

        final Map<String, Object> hashMap = new HashMap<>();
//        hashMap.put("security.protocol", "SASL_SSL");
//
//        hashMap.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//        hashMap.put(SaslConfigs.SASL_JAAS_CONFIG,
//                    String.format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
//                                          "      username=\"%s\" " +
//                                          "      password=\"%s\";", username, password));
        hashMap.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        hashMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        hashMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class);
        hashMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        hashMap.put(JsonDeserializer.TRUSTED_PACKAGES, "com.mq.kafkaconsumer");
        return hashMap;
    }

    private JsonDeserializer<MessageBody> getDeserializer() {
        JsonDeserializer<MessageBody> deserializer = new JsonDeserializer<>(MessageBody.class);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(true);
        return deserializer;
    }

    @Bean
    public ConsumerFactory<String, MessageBody> consumerFactory() {

        return new DefaultKafkaConsumerFactory<>(consumerConfigs(),
                                                 new StringDeserializer(),
                                                 getDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MessageBody>
    getKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, MessageBody> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

}
