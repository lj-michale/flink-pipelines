package com.turing.java.flink20.pipeline.demo11;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.time.ZoneId;

public class Demo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 每20秒作为checkpoint的一个周期
        env.enableCheckpointing(20000);
        // 两次checkpoint间隔最少是10秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        // 程序取消或者停止时不删除checkpoint
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // checkpoint必须在60秒结束,否则将丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只能有一个checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置EXACTLY_ONCE语义,默认就是这个
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint存储位置
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/xxx/data/testData/checkpoint");
        // 设置执行模型为Streaming方式
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("test_topic", "test","localhost:9092");

        SingleOutputStreamOperator<String> sensorDS = env.socketTextStream("localhost", 9999);

//        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
//                // 指定 kafka 的地址和端口
//                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
//                // 指定序列化器 我们是发送方 所以我们是生产者
//                .setRecordSerializer(
//                        KafkaRecordSerializationSchema.<String>builder()
//                                .setTopic("like")
//                                .setValueSerializationSchema(new SimpleStringSchema())
//                                .build()
//                )
//                // 写到 kafka 的一致性级别: 精准一次 / 至少一次
//                // 如果是精准一次
//                //  1.必须开启检查点 env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE)
//                //  2.必须设置事务的前缀
//                //  3.必须设置事务的超时时间: 大于 checkpoint间隔 小于 max 15分钟
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix("lyh-")
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*60*1000+"")
//                .build();
//        sensorDS.sinkTo(kafkaSink);
        sensorDS.sinkTo(FlinkSinkUtil.getKafkaSink("test_topic2","localhost:9092"));

        // todo 输出到文件系统
        FileSink<String> fileSink = FileSink.
                // 泛型方法 需要和输出结果的泛型保持一致
                        <String>forRowFormat(
                        new Path("D:/Desktop"),    // 指定输出路径 可以是 hdfs:// 路径
                        new SimpleStringEncoder<>("UTF-8")) // 指定编码
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("lyh")
                        .withPartSuffix(".log")
                        .build())
                // 按照目录分桶 一个小时一个目录(这里的时间格式别改为分钟 会报错: flink Relative path in absolute URI:)
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 设置文件滚动策略-时间或者大小 10s 或 1KB 或 5min内没有新数据写入 滚动一次
                // 滚动的时候 文件就会更名为我们设定的格式(前缀)不再写入
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10L))  // 10s
                                .withMaxPartSize(new MemorySize(1024)) // 1KB
                                .withInactivityInterval(Duration.ofMinutes(5))  // 5min
                                .build()
                )
                .build();
        env.execute("Flink from Kafka!");
    }
}
