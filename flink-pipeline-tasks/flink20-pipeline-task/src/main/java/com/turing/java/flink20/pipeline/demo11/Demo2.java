package com.turing.java.flink20.pipeline.demo11;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Demo2 {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建kafka Consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id", "groupID");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("test_topic",new SimpleStringSchema(),properties);
        myConsumer.setStartFromLatest();

        // 数据接入
        DataStream<String> dataStream = env.addSource(myConsumer);
        dataStream.print();
        env.execute("Flink from Kafka");
    }
}
