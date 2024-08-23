package com.turing.java.flink20.pipeline.demo9;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event {

    private String user;

    private String url;

    private Long timestamp;
}