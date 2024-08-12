package com.turing.java.flink20.pipeline.demo2;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class TableAPIAndSQLExample {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.inBatchMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);


    }
}
