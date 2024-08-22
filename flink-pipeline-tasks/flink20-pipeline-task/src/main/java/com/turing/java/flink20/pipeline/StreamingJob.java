package com.turing.java.flink20.pipeline;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @descr Flink实时流计算作业
 *
 * */
public class StreamingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);

        // advanced options:
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // only two consecutive checkpoint failures are tolerated
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained
        // after job cancellation
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        // enables the unaligned checkpoints
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        // sets the checkpoint storage where checkpoint snapshots will be written
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs:///my/checkpoint/dir");
        env.configure(config);

        // enable checkpointing with finished tasks
        // Configuration config = new Configuration();
        // config.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        // env.configure(config);

        // State Backends
        // Configuration config = new Configuration();
        // config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        // env.configure(config);

        DataStream<String> textStream = env.socketTextStream("localhost", 9999, "\n");
        DataStream<Tuple2<String, Integer>> wordCountStream = textStream
                // 对数据源的单词进行拆分，每个单词记为1，然后通过out.collect将数据发射到下游算子
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                             @Override
                             public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                 for (String word : value.split("\\s")) {
                                     out.collect(new Tuple2<>(word, 1));
                                 }
                             }
                         }
                )
                // 对单词进行分组
                .keyBy(value -> value.f0)
                // 对某个组里的单词的数量进行滚动相加统计
                .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));

        wordCountStream.print("WordCountStream=======").setParallelism(1);

        env.execute(StreamingJob.class.getSimpleName());

    }

}
