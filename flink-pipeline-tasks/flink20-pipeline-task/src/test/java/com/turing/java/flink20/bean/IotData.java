package com.turing.java.flink20.bean;

import lombok.Data;

import java.util.Map;


@Data
public class IotData {

    private long timestamp;

    private int deviceId;

    public void setData(Map<String, Double> data) {

    }
}
