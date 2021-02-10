package com.kafka.example.consumer.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.kafka.example.consumer.model.Notification;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class LogService {
    
    @KafkaListener(topics = "logging", groupId = "logService", containerFactory = "kafkaListenerContainerFactory")
    public void logNotification (@Payload Notification notification, @Header(KafkaHeaders.RECEIVED_TIMESTAMP) String timestamp){
        
        System.out.println("***********************************");
        System.out.println(buildLogOutput(notification, timestamp));
        System.out.println("***********************************");
    }

    String buildLogOutput (Notification notification, String timestamp){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(timestamp)), ZoneId.systemDefault());
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("MM-dd-yyyy hh:mm:ss");

        return String.format("[%s] %s:%s:%s - %s", 
            dateTimeFormatter.format(localDateTime),
            notification.getService(), 
            notification.getClazz(), 
            notification.getType(), 
            notification.getMessage()
        );
    }

}
