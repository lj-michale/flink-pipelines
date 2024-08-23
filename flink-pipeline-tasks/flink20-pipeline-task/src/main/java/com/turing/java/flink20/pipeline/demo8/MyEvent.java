package com.turing.java.flink20.pipeline.demo8;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MyEvent {

    private long timestamp;

    public boolean hasWatermarkMarker() {
        return true;
    }

    public long getWatermarkTimestamp() {
        return 1L;
    }
}
