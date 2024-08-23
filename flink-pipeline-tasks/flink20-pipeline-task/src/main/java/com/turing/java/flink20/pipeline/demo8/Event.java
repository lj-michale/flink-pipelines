package com.turing.java.flink20.pipeline.demo8;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    private String user;
    private String url;
    private long timestamp;
}
