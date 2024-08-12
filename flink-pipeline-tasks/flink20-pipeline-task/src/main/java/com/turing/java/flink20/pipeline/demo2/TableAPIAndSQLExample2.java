package com.turing.java.flink20.pipeline.demo2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Alternatively, users can create a StreamTableEnvironment from an existing StreamExecutionEnvironment to interoperate with the DataStream API.
 * */
public class TableAPIAndSQLExample2 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    }

}
