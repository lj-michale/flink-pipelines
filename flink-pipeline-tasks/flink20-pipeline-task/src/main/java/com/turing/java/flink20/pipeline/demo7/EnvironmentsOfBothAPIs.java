package com.turing.java.flink20.pipeline.demo7;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneId;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/data_stream_api/
 * */
public class EnvironmentsOfBothAPIs {

    public static void main(String[] args) throws Exception {

        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set the batch runtime mode
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // uncomment this for streaming mode
        // env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // set various configuration early
        // env.setMaxParallelism(256);
        // env.getConfig().addDefaultKryoSerializer(MyCustomType.class, CustomKryoSerializer.class);
        // env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);

        // setup Table API
        // the table environment adopts the runtime mode during initialization
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // set configuration early
        // tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Europe/Berlin"));

        // create a DataStream
        DataStream<String> dataStream = env.fromData("Alice", "Bob", "John");

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream);

        // register the Table object as a view and query it
        tableEnv.createTemporaryView("InputTable", inputTable);
        Table resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable");

        // interpret the insert-only Table as a DataStream again
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

        // add a printing sink and execute in DataStream API
        resultStream.print();

        env.execute(EnvironmentsOfBothAPIs.class.getSimpleName());

    }
}
