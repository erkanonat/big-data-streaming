package com.hvl.iot.bigdatastreaming.service;

import com.hvl.iot.bigdatastreaming.model.IoTData;
import com.hvl.iot.bigdatastreaming.utilities.IoTDataDecoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Service
@Slf4j
public class SparkStreamingService {

    @Value("${com.iot.app.spark.app.name}")
    private String appName;

    @Value("${com.iot.app.spark.master}")
    private String master;

    @Value("${com.iot.app.kafka.zookeeper}")
    private String zookeeperBroker;

    @Value("${com.iot.app.kafka.brokerlist}")
    private String kafkaBroker;

    @Value("${com.iot.app.kafka.topic}")
    private String kafkaIoTEventsTopic;

    @Value("${com.iot.app.spark.checkpoint.dir}")
    private String sparkCheckpointDir;

    @Value("${kafka.iot.group.id}")
    private String iotKafkaGroupId;

    @Autowired
    private SmartCityService smartCityService;

    @PostConstruct
    public void init() {

        try {
            // initialize spark streaming context
            SparkConf conf = new SparkConf()
                    .setAppName(appName)
                    .setMaster(master);

            //batch interval of 5 seconds for incoming stream
            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
            //add check point directory
            jssc.checkpoint(sparkCheckpointDir);

            //read and set Kafka properties
            Map<String, Object> kafkaParams = new HashMap<String, Object>();

            kafkaParams.put("zookeeper.connect", zookeeperBroker);
            kafkaParams.put("bootstrap.servers", kafkaBroker);
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", IoTDataDecoder.class);
//            kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
            kafkaParams.put("group.id", iotKafkaGroupId);

            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);

            Set<String> topicsSet = new HashSet<String>();
            topicsSet.add(kafkaIoTEventsTopic);

            //create direct kafka stream
            JavaDStream<ConsumerRecord<String, IoTData>> directKafkaStream =
                    KafkaUtils.createDirectStream(
                            jssc, LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, IoTData>Subscribe(topicsSet, kafkaParams));

            log.info("Starting Stream Processing");

            directKafkaStream.cache();

            //process data
            // TODO : process data methods implement and call
            smartCityService.processWindowTrafficData(directKafkaStream);

            jssc.start();
            jssc.awaitTermination();


        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
