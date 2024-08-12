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
// * �ο� streamNative pulsar flink demo
// * <a href="https://github.com/streamnative/examples/tree/master/pulsar-flink">pulsar-flink example</a>
// * �����Ϸ����ӵ� streamNative flink demo ʹ�� 1.10.1 �汾 flink �Լ� 2.4.17 �汾 pulsar connector,
// * �뵱ǰ 1.20 �����汾�� flink �� pulsar connector api �Ѿ����ڲ��� api ����
// * ��˱� demo ʹ�� 1.17 flink �汾�����ع�
// * <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/resource-providers/standalone/overview/">1.17 flink doc</a>
// * <p>
// * demo ͳ��ʱ�䴰����Դ topic ������Ϣ��ÿ�����ʳ���Ƶ�ʴ���
// * ���ҽ�ͳ�ƽ������ÿ�����ʶ�Ӧһ����Ϣ�ĸ�ʽ�����л�����Ϣ��Ͷ�ݵ�Ŀ�� topic ��
// *
// */
//public class PulsarStreamingWordCount {
//
//    private static final Logger LOG = LoggerFactory.getLogger(PulsarStreamingWordCount.class);
//
//    public static void main(String[] args) throws Exception {
//        //  �������񴫲�
//        //  Ĭ��ʹ�� authToken ��ʽ��Ȩ
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
//        //  �˴��� message �е� payload ���л����ַ�������
//        //  Ŀǰ source ֻ֧�ֽ�����Ϣ payload �е����ݣ��� payload �е����ݽ����� pulsar schema ��������Զ���� class ����
//        //  ���޷����� message �� properties �е��������ԣ����� publish_time
//        //  �����Ҫ���� message �е� properties����Ҫ�ڼ̳�����ʵ�� PulsarDeserializationSchema.getProducedType() ����
//        //  getProducedType �������ʵ�ֽ�Ϊ��������Ҫ����ÿ�������л��������
//        //
//        //https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/pulsar/#deserializer
//            .setDeserializationSchema(new
//                       SimpleStringSchema())
//            .setSubscriptionName(subscriptionName)
//            .setAuthentication("org.apache.pulsar.client.impl.auth.AuthenticationToken", token)
//            .build();
//        //  ���ڴ˴�û��ʹ����Ϣ���е�ʱ�䣬��û��ʹ����Ϣ�� publish_time
//        //  ��˴˴�ʹ�� noWatermark ģʽ��ʹ�� taskManager ��ʱ����Ϊʱ�䴰��
//        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source");
//        //  process
//        //  ���� source ��ÿ����Ϣ��ͨ���ո�ָ�ɵ������ʣ�֮����л�۴����ҳ�ʼ���� WordCount �ṹ��
//        //  ����ʹ�� TumblingProcessingTimeWindows����ʹ�õ�ǰ taskManager ϵͳʱ�����ʱ�䴰��
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
//        //  Ŀǰ 1.17 flink ���л��ṩ�������Ѿ�ʵ�ֵķ�����һ����ʹ�� pulsar ���� schema����һ����ʹ�� flink �� schema
//        //  ������Ŀǰ tdmq pulsar �ṩ���� 2.9 �汾�� pulsar������ schema ֧�ֻ���������
//        //  �˴�ʹ�� flink PulsarSerializationSchema<T> �ṩ�Ľӿڣ���ǰ��Ҫ��Ҫʵ�� serialize(IN element, PulsarSinkContext sinkContext) ����
////  ������� IN �����Զ������л�Ϊ byte ����
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
////  �˴��� wordCount ��Ӵ���ʱ��󣬽� wordCount ʹ�� json ��ʽ���л�Ϊ byte ����
////  �Ա��ܹ�ֱ�Ӳ鿴��Ϣ������
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
