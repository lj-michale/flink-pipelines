package com.turing.java.flink20.pipeline.demo8;

import org.apache.flink.api.common.functions.MapFunction;

public  class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        // 假设传入的字符串是传感器ID和水位的逗号分隔值，解析并返回水位
        String[] parts = value.split(",");
        WaterSensor waterSensor = new WaterSensor();
        waterSensor.setId(parts[0]);
        return waterSensor;
    }
}