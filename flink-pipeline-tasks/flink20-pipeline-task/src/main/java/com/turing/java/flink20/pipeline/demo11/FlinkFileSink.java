package com.turing.java.flink20.pipeline.demo11;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * @descr Flink中FileSink的使用
 * https://blog.csdn.net/AnameJL/article/details/131416376
 * */
public class FlinkFileSink {
    public static void main(String[] args) throws Exception {
        // 构建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);
        // 这里是生成数据流,CustomizeSource这个类是自定义数据源(为了方便测试)
        SingleOutputStreamOperator<String> dataStreamSource = env.socketTextStream("localhost", 9999);
//        DataStreamSource<CustomizeBean> dataStreamSource = env.addSource(new CustomizeSource());
        // 现将数据转换成字符串形式
        SingleOutputStreamOperator<String> map = dataStreamSource.map(bean -> bean.toString());
        // 构造FileSink对象,这里使用的RowFormat,即行处理类型的
        FileSink<String> fileSink = FileSink
                // 配置文件输出路径及编码格式
                .forRowFormat(new Path("/Users/xxx/data/testData/"), new SimpleStringEncoder<String>("UTF-8"))
                // 设置文件滚动策略(文件切换)
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(180)) // 设置间隔时长180秒进行文件切换
                                .withInactivityInterval(Duration.ofSeconds(20)) // 文件20秒没有数据写入进行文件切换
                                .withMaxPartSize(MemorySize.ofMebiBytes(1)) // 设置文件大小1MB进行文件切换
                                .build()
                )
                // 分桶策略(划分子文件夹)
                .withBucketAssigner(new DateTimeBucketAssigner<String>()) // 按照yyyy-mm-dd--h进行分桶
                //设置分桶检查时间间隔为100毫秒
                .withBucketCheckInterval(100)
                // 输出文件文件名相关配置
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("test_") // 文件前缀
                                .withPartSuffix(".txt") // 文件后缀
                                .build()
                )
                .build();
        // 输出到文件
        map.print();
        map.sinkTo(fileSink);
        env.execute();
    }
}
