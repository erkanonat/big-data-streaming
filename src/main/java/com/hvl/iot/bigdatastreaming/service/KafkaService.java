package com.hvl.iot.bigdatastreaming.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.hvl.iot.bigdatastreaming.model.IoTData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Slf4j
public class KafkaService {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    private SmartCityService smartCityService;

    @Value("${KAFKA_IOT_EVENTS_TOPIC:Alarm}")
    private String kafkaMessageTopic;

    public void publish(final String message) {

        log.info("Message published: {}" , message);
        kafkaTemplate.send(kafkaMessageTopic, message);

    }

//    @KafkaListener(topics = "${kafka.message.topic}", groupId = "${kafka.group.id}")
//    public void onMessage(String message) {
//
//        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//
//        try {
//
//            Gson gson = new GsonBuilder().setPrettyPrinting().create();
//            JsonParser jp = new JsonParser();
//            JsonElement je = jp.parse(message);
//            String prettyJsonString = gson.toJson(je);
//
//            log.warn("Kafka Received message: " + prettyJsonString);
//
//            IoTData receivedIoTEvent = mapper.readValue(message, IoTData.class);
//
//            smartCityService.handleIoTEvent(receivedIoTEvent);
//
//
//        } catch (IOException e) {
//            log.error("Error while parsing alarm object {}", e.getStackTrace());
//        }
//
//    }

}
