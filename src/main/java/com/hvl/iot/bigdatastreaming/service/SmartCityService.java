package com.hvl.iot.bigdatastreaming.service;

import com.hvl.iot.bigdatastreaming.model.AggregateKey;
import com.hvl.iot.bigdatastreaming.model.IoTData;
import com.hvl.iot.bigdatastreaming.model.WindowTrafficData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

@Service
@Slf4j
public class SmartCityService implements Serializable {


    @Autowired
    private KafkaService kafkaService;

    /**
     * Method to get window traffic counts of different type of vehicles for each route.
     * Window duration = 30 seconds and Slide interval = 10 seconds
     *
     * @param filteredIotDataStream IoT data stream
     */
    public void processWindowTrafficData(JavaDStream<ConsumerRecord<String, IoTData>> filteredIotDataStream) {

        // reduce by key and window (30 sec window and 10 sec slide).
        JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredIotDataStream
                .mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.value().getPtsId(), iot.value().getVehicleType()), 1L))
                .reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(30), Durations.seconds(10));

        // Transform to dstream of TrafficData
        JavaDStream<WindowTrafficData> trafficDStream = countDStreamPair.map(windowTrafficDataFunc);

        // send to kafka  to  store in druid db
//        trafficDStream.foreachRDD(windowTrafficDataJavaRDD -> kafkaService.publish(windowTrafficDataJavaRDD.toString()));

        trafficDStream.foreachRDD(new VoidFunction<JavaRDD<WindowTrafficData>>() {
            @Override
            public void call(JavaRDD<WindowTrafficData> windowTrafficDataJavaRDD) throws Exception {
                windowTrafficDataJavaRDD.foreach(new VoidFunction<WindowTrafficData>() {
                    @Override
                    public void call(WindowTrafficData windowTrafficData) throws Exception {
                        System.out.println("publish to kafka : "+ windowTrafficData.toString());


                    }
                });
            }
        });

    }


    //Function to create WindowTrafficData object from IoT data
    private static final Function<Tuple2<AggregateKey, Long>, WindowTrafficData> windowTrafficDataFunc = (tuple -> {

        log.info("Window Count : " + "key " + tuple._1().getPtsId() + "-" + tuple._1().getVehicleType()+ " value " + tuple._2());

        WindowTrafficData trafficData = new WindowTrafficData();
        trafficData.setPtsId(tuple._1().getPtsId());
        trafficData.setVehicleType(tuple._1().getVehicleType());
        trafficData.setTotalCount(tuple._2());
        trafficData.setTimeStamp(new Date());
        trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        return trafficData;
    });


}
