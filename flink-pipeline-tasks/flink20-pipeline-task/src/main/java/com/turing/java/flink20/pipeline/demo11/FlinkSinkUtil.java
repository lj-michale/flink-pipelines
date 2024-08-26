package com.turing.java.flink20.pipeline.demo11;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class FlinkSinkUtil {

    public static KafkaSink<String> getKafkaSink(String sinkTopic, String kafkaAdress){
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaAdress)
                //写入的精准一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(sinkTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        return kafkaSink;
    }

    /**
     * 获取upsert-kafka连接器的连接属性
     *         tableEnv.executeSql(
     *                 "create table student(\n" +
     *                         " id int,\n" +
     *                         " name string,\n"
     *                         "  PRIMARY KEY (id) NOT ENFORCED\n" +
     *                         ")"
     *                  + SQLUtil.getUpsertKafkaDDL("test_topic","localhost:9092"));
     *         tableEnv.executeSql("insert into student select * from "+tableResult);
     *         tableEnv.executeSql("select * from student").print();
     *
     * */
    public static String getUpsertKafkaDDL(String sinkTopic,String kafkaAdress) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + sinkTopic + "',\n" +
                "  'properties.bootstrap.servers' = '" + kafkaAdress + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

}
