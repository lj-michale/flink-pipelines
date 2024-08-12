//package com.turing.java.flink20.pipeline;
//
//import com.turing.java.flink20.bean.WordCount;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.serialization.SerializationSchema;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.connector.base.DeliveryGuarantee;
//import org.apache.flink.connector.pulsar.sink.PulsarSink;
//import org.apache.flink.connector.pulsar.sink.PulsarSinkOptions;
//import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
//import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
//import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessage;
//import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
//import org.apache.flink.connector.pulsar.source.PulsarSource;
//import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.time.LocalDateTime;
//
//
///**
// *  https://blog.csdn.net/qq_36668144/article/details/139438844?utm_medium=distribute.pc_relevant.none-task-blog-2~default~baidujs_baidulandingword~default-0-139438844-blog-125610029.235^v43^pc_blog_bottom_relevance_base1&spm=1001.2101.3001.4242.1&utm_relevant_index=3
// *
// * 参考 streamNative pulsar flink demo
// * <a href="https://github.com/streamnative/examples/tree/master/pulsar-flink">pulsar-flink example</a>
// * 由于上方链接的 streamNative flink demo 使用 1.10.1 版本 flink 以及 2.4.17 版本 pulsar connector,
// * 与当前 1.20 社区版本的 flink 和 pulsar connector api 已经存在部分 api 差异
// * 因此本 demo 使用 1.17 flink 版本进行重构
// * <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/resource-providers/standalone/overview/">1.17 flink doc</a>
// * <p>
// * demo 统计时间窗口内源 topic 所有消息中每个单词出现频率次数
// * 并且将统计结果按照每个单词对应一条消息的格式，序列化后消息后投递到目标 topic 中
// *
// */
//public class PulsarStreamingWordCount {
//
//    private static final Logger LOG = LoggerFactory.getLogger(PulsarStreamingWordCount.class);
//
//    public static void main(String[] args) throws Exception {
//        //  解析任务传参
//        //  默认使用 authToken 方式鉴权
//        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        if (parameterTool.getNumberOfParameters() < 2) {
//            System.err.println("Missing parameters!");
//            return;
//        }
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
//        env.enableCheckpointing(60000);
//        env.getConfig().setGlobalJobParameters(parameterTool);
//        String brokerServiceUrl = parameterTool.getRequired("broker-service-url");
//        String inputTopic = parameterTool.getRequired("input-topic");
//        String outputTopic = parameterTool.getRequired("output-topic");
//        String subscriptionName = parameterTool.get("subscription-name", "WordCountTest");
//        String token = parameterTool.getRequired("token");
//        int timeWindowSecond = parameterTool.getInt("time-window", 60);
//        //  source
//        PulsarSource<String> source =
//                PulsarSource.builder()
//                        .setServiceUrl(brokerServiceUrl)
//                        .setStartCursor(StartCursor.latest())
//                        .setTopics(inputTopic)
//        //  此处将 message 中的 payload 序列化成字符串类型
//        //  目前 source 只支持解析消息 payload 中的内容，将 payload 中的内容解析成 pulsar schema 对象或者自定义的 class 对象
//        //  而无法解析 message 中 properties 中的其他属性，例如 publish_time
//        //  如果需要解析 message 中的 properties，需要在继承类中实现 PulsarDeserializationSchema.getProducedType() 方法
//        //  getProducedType 这个方法实现较为繁琐，需要声明每个反序列化后的属性
//        //
//        //https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/pulsar/#deserializer
//            .setDeserializationSchema(new
//                       SimpleStringSchema())
//            .setSubscriptionName(subscriptionName)
//            .setAuthentication("org.apache.pulsar.client.impl.auth.AuthenticationToken", token)
//            .build();
//        //  由于此处没有使用消息体中的时间，即没有使用消息的 publish_time
//        //  因此此处使用 noWatermark 模式，使用 taskManager 的时间作为时间窗口
//        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source");
//        //  process
//        //  解析 source 中每行消息，通过空格分割成单个单词，之后进行汇聚处理并且初始化成 WordCount 结构体
//        //  这里使用 TumblingProcessingTimeWindows，即使用当前 taskManager 系统时间计算时间窗口
//        DataStream<WordCount> wc = stream
//                .flatMap((FlatMapFunction<String, WordCount>) (line, collector) -> {
//                    LOG.info("current line = {}, word list = {}", line, line.split("\\s"));
//                    for (String word : line.split("\\s")) {
//                        collector.collect(new
//                                WordCount(word, 1, null));
//                    }
//                })
//                .returns(WordCount.class)
//                .keyBy(WordCount::getWord)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(timeWindowSecond)))
//                .reduce((ReduceFunction<WordCount>) (c1, c2) -> {
//                    WordCount reducedWordCount = new WordCount(c1.getWord(), c1.getCount() + c2.getCount(), null);
//                    LOG.info("previous [{}] [{}], current wordCount {}", c1, c2, reducedWordCount);
//                    return reducedWordCount;
//                });
//        //  sink
//        //  目前 1.17 flink 序列化提供了两种已经实现的方法，一种是使用 pulsar 内置 schema，另一种是使用 flink 的 schema
//        //  但由于目前 tdmq pulsar 提供的是 2.9 版本的 pulsar，对于 schema 支持还不够完善
//        //  此处使用 flink PulsarSerializationSchema<T> 提供的接口，当前主要需要实现 serialize(IN element, PulsarSinkContext sinkContext) 方法
////  将传入的 IN 对象自定义序列化为 byte 数组
////  https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/pulsar/#serializer
//        PulsarSink<WordCount> sink = PulsarSink.builder()
//                .setServiceUrl(brokerServiceUrl)
//                .setTopics(outputTopic)
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .setAuthentication("org.apache.pulsar.client.impl.auth.AuthenticationToken", token)
//                .setConfig(PulsarSinkOptions.PULSAR_BATCHING_ENABLED, false)
//                .setSerializationSchema(new PulsarSerializationSchema<WordCount>() {
//                    private ObjectMapper objectMapper;
//                    @Override
//                    public void open(
//                            SerializationSchema.InitializationContext initializationContext,
//                            PulsarSinkContext sinkContext,
//                            SinkConfiguration sinkConfiguration)
//                            throws Exception {
//                        objectMapper = new ObjectMapper();
//                    }
//                    @Override
//                    public PulsarMessage<?> serialize(WordCount wordCount, PulsarSinkContext sinkContext) {
////  此处将 wordCount 添加处理时间后，将 wordCount 使用 json 方式序列化为 byte 数组
////  以便能够直接查看消息体内容
//                        byte[] wordCountBytes;
//                        wordCount.setSinkDateTime(LocalDateTime.now().toString());
//                        try {
//                            wordCountBytes = objectMapper.writeValueAsBytes(wordCount);
//                        } catch (Exception exception) {
//                            wordCountBytes = exception.getMessage().getBytes();
//                        }
//                        return PulsarMessage.builder(wordCountBytes).build();
//                    }
//                })
//                .build();
//        wc.sinkTo(sink);
//        env.execute("Pulsar Streaming WordCount");
//    }
//}
//
