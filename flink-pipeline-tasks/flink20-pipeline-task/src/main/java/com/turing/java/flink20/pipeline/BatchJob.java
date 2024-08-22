package com.turing.java.flink20.pipeline;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

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

        DataStream<String> textStream = env.fromData(Arrays.asList(
                "java,c++,php,java,spring",
                "hadoop,scala",
                "c++,jvm,html,php"
        ));

        DataStream<Tuple2<String, Integer>> wordCountStream = textStream
                // 对数据源的单词进行拆分，每个单词记为1，然后通过out.collect将数据发射到下游算子
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                             @Override
                             public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                 for (String word : value.split(",")) {
                                     out.collect(new Tuple2<>(word, 1));
                                 }
                             }
                         }
                )
                // 对单词进行分组
                .keyBy(value -> value.f0)
                // 对某个组里的单词的数量进行滚动相加统计
                .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));
        wordCountStream.print("WordCountBatch========").setParallelism(1);
        env.execute(BatchJob.class.getSimpleName());
    }

}
