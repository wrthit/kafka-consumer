package com.kafka.example.consumer.service;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import com.kafka.example.consumer.configuration.KafkaConsumerConfiguration;
import com.kafka.example.consumer.model.LogType;
import com.kafka.example.consumer.model.Notification;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@EmbeddedKafka(topics = { "logging" }, partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092",
        "port=9092" })
@ContextConfiguration(classes = {KafkaConsumerConfiguration.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LogServiceIT {
    
    @SpyBean
    private LogService logServiceMock;

    @Autowired
    private EmbeddedKafkaBroker kafkaEmbedded;

    Producer<String, Notification> producer;
    private static final String TOPIC = "logging";

    @BeforeAll
    public void setup(){
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(kafkaEmbedded));
        // create a Kafka consumer factory
        producer = new DefaultKafkaProducerFactory<String, Notification>(configs, new StringSerializer(), new JsonSerializer<>()).createProducer();
        
    }

    @AfterAll
    public void teardown(){
        if(producer != null){
            producer.close();
        }
    }

    @Test
    public void kafkaListenerConfiguredCorrectly(){
        Notification notification = Notification.builder()
                                                .service("some service name")
                                                .clazz("some class name")
                                                .message("this is a test")
                                                .type(LogType.INFO)
                                                .build();

        producer.send(new ProducerRecord<String, Notification>(TOPIC, notification));
        
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            fail("InterruptException thrown. " + e.getMessage());
        }

        verify(logServiceMock, atLeast(1)).logNotification(any(Notification.class), anyString());
    }

}