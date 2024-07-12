package com.turing.java.flink19.utils;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @descri flink1.19执行环境初始化
 *
 * @author lj.michale
 * @date 2023-11-07
 */
@Builder
@Data
public class FlinkEnvUtils {

    /**
     * 设置状态后端为 HashMapStateBackend
     *
     * @param env env
     */
    public static void setHashMapStateBackend(StreamExecutionEnvironment env) {
        setCheckpointConfig(env);
        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapStateBackend);
    }

    /**
     * 设置状态后端为 EmbeddedRocksDBStateBackend
     *
     * @param env env
     */
    public static void setEmbeddedRocksDBStateBackend(StreamExecutionEnvironment env) {
        setCheckpointConfig(env);
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        embeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(embeddedRocksDBStateBackend);
    }

    /**
     * Checkpoint参数相关配置
     *
     * @param env env
     */
    public static void setCheckpointConfig(StreamExecutionEnvironment env) {
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
    }

    /**
     * 流式：获取getStreamTableEnv
     *
     * @param args args
     */
    public static FlinkEnv getStreamTableEnv(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Configuration configuration = Configuration.fromMap(parameterTool.toMap());
        configuration.setString("rest.flamegraph.enabled", "true");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        String stateBackend = parameterTool.get("state.backend", "rocksdb");
        /** 判断状态后端模式 */
        if ("hashmap".equals(stateBackend)) {
            setHashMapStateBackend(env);
        } else if ("rocksdb".equals(stateBackend)) {
            setEmbeddedRocksDBStateBackend(env);
        }
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(6,
                Time.of(10L, TimeUnit.MINUTES), Time.of(5L, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);

        EnvironmentSettings settings =
                EnvironmentSettings
                        .newInstance()
                        .inStreamingMode()
                        .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.getConfig().addConfiguration(configuration);
        FlinkEnv flinkEnv =
                FlinkEnv.builder()
                        .streamExecutionEnvironment(env)
                        .streamTableEnvironment(tEnv)
                        .build();

        return flinkEnv;
    }

    /**
     * 批式：获取getBatchTableEnv
     *
     * @param args args
     */
    public static FlinkEnv getBatchTableEnv(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Configuration configuration = Configuration.fromMap(parameterTool.toMap());
        configuration.setString("rest.flamegraph.enabled", "true");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        String stateBackend = parameterTool.get("state.backend", "rocksdb");
        // 判断状态后端模式
        if ("hashmap".equals(stateBackend)) {
            setHashMapStateBackend(env);
        } else if ("rocksdb".equals(stateBackend)) {
            setEmbeddedRocksDBStateBackend(env);
        }
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(6,
                Time.of(10L, TimeUnit.MINUTES), Time.of(5L, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);

        EnvironmentSettings settings =
                EnvironmentSettings
                        .newInstance()
                        .inBatchMode()
                        .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.getConfig().addConfiguration(configuration);
        FlinkEnv flinkEnv =
                FlinkEnv.builder()
                        .streamExecutionEnvironment(env)
                        .streamTableEnvironment(tEnv)
                        .build();

        return flinkEnv;
    }



    @Builder
    @Data
    public static class FlinkEnv {
        private StreamExecutionEnvironment streamExecutionEnvironment;

        /**
         * StreamTableEnvironment用于流计算场景，流计算的对象是DataStream。相比TableEnvironment，StreamTableEnvironment 提供了DataStream和Table之间相互转换的接口，
         * 如果用户的程序除了使用Table API & SQL编写外，还需要使用到DataStream API，则需要使用StreamTableEnvironment。
         */
        private StreamTableEnvironment streamTableEnvironment;

        private TableEnvironment tableEnvironment;

        public StreamTableEnvironment streamTEnv() {
            return this.streamTableEnvironment;
        }

        public TableEnvironment batchTEnv() {
            return this.tableEnvironment;
        }

        public StreamExecutionEnvironment env() {
            return this.streamExecutionEnvironment;
        }
    }

}