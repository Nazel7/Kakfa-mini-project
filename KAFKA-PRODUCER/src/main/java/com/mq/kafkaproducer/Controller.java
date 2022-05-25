package com.mq.kafkaproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.KafkaException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("send-message")
public class Controller {

    private final Logger log= LoggerFactory.getLogger(Controller.class);
    @Autowired
    private PushNotificationService mNotificationService;

    public static final String TOPIC= "kafka_topic_Q";

    @PostMapping(" ")
    public MessageBody sendMessage(@RequestBody final MessageBody messageBody){

        log.info("::: Sending message.....");
        if (messageBody == null){
            throw new KafkaException("::: Message failed to send :::");
        }

        try {
          mNotificationService.sendMessage(messageBody);
            log.info("::: Message sent successfully :::");

            return messageBody;
        }catch (Exception ex){
            throw new KafkaException("::: Message failed to send :::");
        }

    }
}
