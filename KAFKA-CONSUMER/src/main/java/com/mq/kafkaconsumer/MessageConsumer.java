package com.mq.kafkaconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class MessageConsumer {

    private final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    @KafkaListener(topics = KafkaConfig.TOPIC, groupId = KafkaConfig.GROUP_ID,
            containerFactory = "getKafkaListenerContainerFactory")
    public void consumer(@Payload MessageBody messageBody) {

        System.out.println(":::::::::: "+ messageBody);
        log.info("::: Receiving message....");

        if (messageBody == null) {
            throw new KafkaException("::: Receiving Message failed....");
        }

        try {
            System.out.println("Message received has body: " + messageBody);

        } catch (Exception ex) {
            throw new KafkaException("::: Receiving Message Failed....");
        }
    }
}
