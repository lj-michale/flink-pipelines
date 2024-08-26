package com.turing.java.flink20.pipeline.demo11;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;

/**
 * @descr
 * */
public class FlinkSourceUtil {

    public static KafkaSource<String> getKafkaSource(String topic,
                                                     String groupId,
                                                     String kafkaAdress) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaAdress)
                .setTopics(topic)
                .setGroupId(groupId)
                //初始化读取最新数据
                .setStartingOffsets(OffsetsInitializer.earliest())
                //如下这种方式不能处理空消息
                // .setValueOnlyDeserializer(new SimpleStringSchema())
                //为了处理读取到空消息的情况，需要自定义反序列化器
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if (message != null) {
                                    return new String(message);
                                }
                                return null;
                            }
                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                ).build();

        return kafkaSource;
    }

    /**
     * 获取kafka连接器的连接属性
     * //从test_topic主题中读取数据  并创建动态表
     *     public void readOdsDb(StreamTableEnvironment tableEnv, String groupId) {
     *         tableEnv.executeSql("CREATE TABLE test_topic(\n" +
     *                 "  `database` string,\n" +
     *                 "  `table` string,\n" +
     *                 "  `type` string,\n" +
     *                 "  `data` map<string,string>,\n" +
     *                 "  `old` map<string,string>,\n" +
     *                 "  `ts` bigint,\n" +
     *                 "  `pt` as proctime(),\n" +
     *                 "  `et` as TO_TIMESTAMP_LTZ(ts, 0),\n" +
     *                 "  WATERMARK FOR `et` AS `et`\n" +
     *                 ") " + SQLUtil.getKafkaDDL(test_topic, groupId));
     *     }
     *
     * */
    public static String getKafkaDDL(String topic,
                                     String groupId,
                                     String kafkaAdress) {
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + kafkaAdress + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }
}
