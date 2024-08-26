//package com.turing.java.flink20.pipeline.demo11;
//
//import org.apache.flink.api.common.serialization.SimpleStringEncoder;
//import org.apache.flink.configuration.MemorySize;
//import org.apache.flink.connector.file.sink.FileSink;
//import org.apache.flink.connector.file.sink.compactor.ConcatFileCompactor;
//import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
//import org.apache.flink.connector.file.sink.compactor.IdenticalFileCompactor;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.core.io.SimpleVersionedSerializer;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
//import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
//import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
//import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
//
//import java.io.IOException;
//import java.time.Duration;
//import java.time.Instant;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.time.format.DateTimeFormatter;
//import java.util.Arrays;
//
///**
// * 描述：flink集成FileSink，forBulkFormat列模式
// *
// */
//public class FileBulkSinkStreaming {
//
//    public static void main(String[] args) throws Exception {
//
//        //=============1.分桶策略=========================================
//        // 自定义分桶策略
//        BucketAssigner<String, String> customBucketAssigner = new BucketAssigner<>() {
//            @Override
//            public String getBucketId(String element, Context context) {
//                // 获取当前时间戳（以秒为单位）
//                long timestamp = System.currentTimeMillis() / 1000;
//                // 将时间戳转换为 LocalDateTime 对象
//                LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.systemDefault());
//                // 定义日期时间格式
//                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
//                // 格式化日期时间对象为指定格式的字符串
//                return formatter.format(dateTime);
//            }
//
//            @Override
//            public SimpleVersionedSerializer<String> getSerializer() {
//                return SimpleVersionedStringSerializer.INSTANCE;
//            }
//        };
//        // 默认基于时间的窗口分桶策略
////        BucketAssigner<String, String> customBucketAssigner = new DateTimeBucketAssigner<>("yyyy-MM-dd--HH");
//
//        //=============2.滚动策略===============================================
//        CheckpointRollingPolicy<String,String> rollingPolicy = new CheckpointRollingPolicy<>() {
//            @Override
//            public boolean shouldRollOnEvent(PartFileInfo<String> partFileInfo, String s) throws IOException {
//                return false;
//            }
//
//            @Override
//            public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileInfo, long l) throws IOException {
//                return false;
//            }
//        };
//
//        //================3.文件命名策略==================================
//        OutputFileConfig outputFileConfig = OutputFileConfig.builder().withPartPrefix("Flink_").withPartSuffix(".dat").build();
//
//        //================4.合并策略======================================
//        FileCompactStrategy fileCompactStrategy = FileCompactStrategy.Builder.newBuilder().enableCompactionOnCheckpoint(1).setNumCompactThreads(1).build();
//        //合并算法，3种
//        //第1种：可以自定义两个文件直接的分割符，由构造方法传入
//        ConcatFileCompactor fileCompactor = new ConcatFileCompactor();
//        //第2种：直接复制一个文件的内容，到另一个文件，一次只能复制一个文件；
////        IdenticalFileCompactor fileCompactor = new IdenticalFileCompactor();
//        //第3种：自定义内容比较多
////        RecordWiseFileCompactor.Reader.Factory<String> stringFactory = new RecordWiseFileCompactor.Reader.Factory<>() {
////            @Override
////            public RecordWiseFileCompactor.Reader<String> createFor(Path path) throws IOException {
////                //需自定义
////                return null;
////            }
////        };
////        RecordWiseFileCompactor fileCompactor = new RecordWiseFileCompactor(stringFactory);
//
//        // 创建FileSink
//        FileSink<String> fileSink = FileSink
//                //指定文件目录与文件写入编码格式
//                .forBulkFormat(new Path("D:\\flink"), new CustomBulkWriterFactory())
//                //设置合并策略
//                .enableCompact(fileCompactStrategy, fileCompactor)
//                //分桶策略，不设置采用默认的分桶策略DateTimeBucketAssigner,基于时间的分配器，每小时产生一个桶，即yyyy-mm-dd-hh
//                .withBucketAssigner(customBucketAssigner)
//                //指定文件前后缀输出
//                .withOutputFileConfig(outputFileConfig)
//                //默认滚动策略，每隔多久把临时文件合并一次
////                .withBucketCheckInterval(1000)
//                //自定义滚动策略，三个条件满足任意一个都会滚动
//                .withRollingPolicy(rollingPolicy)
//                .build();
//
//        // 创建 执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 创建简单批模式数据源
//        DataStreamSource<String> dataStreamSource = env.fromCollection(Arrays.asList("运维测试", "运维开发", "追风的少年"));
//
//        // 把数据源的全部数据写入到文件，注意一旦开启文件合并，则必须设置uid，否则直接启动报错
//        dataStreamSource.sinkTo(fileSink).uid("2");
//
//        // 执行任务
//        env.execute("FileRowSinkStreaming");
//    }
//}
