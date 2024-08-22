package com.turing.java.flink20.pipeline.demo4;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaDataStreamSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.20.1.26:9092")
                .setGroupId("group-flinkdemo")
                .setTopics("topic-flinkdemo")
                // 从最末尾位点开始消费
                .setStartingOffsets(OffsetsInitializer.latest())
                // 从上次消费者提交的地方开始消费，应该采用这种方式，防止服务重启的期间丢失数据
//                .setStartingOffsets(OffsetsInitializer.committedOffsets())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");

        DataStream<Tuple2<String, Integer>> dataStream = dataStreamSource
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : value.split("\\,")) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));

        dataStream.print("BlogDemoStream=======")
                .setParallelism(1);

        env.execute(KafkaDataStreamSourceDemo.class.getSimpleName());
    }
}
