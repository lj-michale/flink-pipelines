package com.turing.java.flink20.pipeline.demo8;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
 *     .setBootstrapServers(brokers)
 *     .setTopics("my-topic")
 *     .setGroupId("my-group")
 *     .setStartingOffsets(OffsetsInitializer.earliest())
 *     .setValueOnlyDeserializer(new SimpleStringSchema())
 *     .build();
 *
 * DataStream<String> stream = env.fromSource(
 *     kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "mySource");
 * */
public class PunctuatedAssigner implements WatermarkGenerator<MyEvent> {

    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
        if (event.hasWatermarkMarker()) {
            output.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // don't need to do anything because we emit in reaction to events above
    }
}