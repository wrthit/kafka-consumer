package com.kafka.example.consumer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Notification {
    private String service;
    
    @JsonProperty("class")
    private String clazz;
    private String message;

    @Builder.Default
    private LogType type = LogType.INFO;
}
