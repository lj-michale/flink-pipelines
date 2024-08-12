package com.turing.java.flink20.pipeline;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @descr Flink周期批计算作业
 * $ bin/flink run -Dexecution.runtime-mode=BATCH <jarFile>
 *
 * STREAMING: The classic DataStream execution mode (default)
 * BATCH: Batch-style execution on the DataStream API
 * AUTOMATIC: Let the system decide based on the boundedness of the sources
 * */
public class BatchJob {

    public static void main(String[] args) throws Exception {
        // Handling Application Parameters
        String propertiesFilePath = "/home/sam/flink/myjob.properties";
        ParameterTool parameters = ParameterTool.fromPropertiesFile(propertiesFilePath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        env.execute();
    }

}
