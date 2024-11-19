package com.turing.java.flink19.pipeline.demo10;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * �������˹��ܣ���������Ҳ��ͦ�򵥵ģ���Ҫ�õ���֪ʶ���� Watermark��״̬�����������ϲ� ��
 * */
public class DuiZhangshanPipeline {

    private static final OutputTag unmatchedPayEventOutputTag = new OutputTag<OrderEvent>("unmatched-pay"){};
    private static final OutputTag unmatchedReceiptEventOutputTag = new OutputTag<ReceiptEvent>("unmatched-receipt"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        // 1. ��ȡ�����¼�����
        DataStream<String> inputOrderStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\my-gitlib\\shishi-daping\\dip\\shishi-daping\\NFDWSYYBigScreen\\TestJsonDmon\\src\\main\\resources\\OrderLog.csv");
        KeyedStream<OrderEvent,String> orderDataStream = inputOrderStream.map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String s) throws Exception {
                        String[] dataArray = s.split(",");
                        return new OrderEvent(Long.parseLong(dataArray[0]),dataArray[1],dataArray[2],Long.parseLong(dataArray[3]));
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
                        return element.getTimestamp()*1000L;
                    }
                }).filter(order -> order.getAction().equals("pay"))
                .keyBy(order -> order.getOrId());

        // 2. ��ȡ�����¼�����
        DataStream<String> inputReceipStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\my-gitlib\\shishi-daping\\dip\\shishi-daping\\NFDWSYYBigScreen\\TestJsonDmon\\src\\main\\resources\\ReceiptLog.csv");
        KeyedStream<ReceiptEvent,String> receipDataStream = inputReceipStream.map(new MapFunction<String, ReceiptEvent>() {
            @Override
            public ReceiptEvent map(String s) throws Exception {
                String[] dataArray = s.split(",");
                return new ReceiptEvent(dataArray[0],dataArray[1],Long.parseLong(dataArray[2]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ReceiptEvent element) {
                return element.getTimestamp()*1000L;
            }
        }).keyBy(order -> order.getOrId());

        // -------------------------------��������-------------------------------------------------
//        DataStream resultStream = orderDataStream.intervalJoin(receipDataStream)  //����ʹ����Թ���
//                .between(Time.seconds(-3), Time.seconds(5))  // �������ݵȴ���������ʱ��ǰ���뵽����������
//                .process(new OrderMatchWithJoinFunction());  // �Զ�������������ϱ�����������

        //�ϲ������������д���
        SingleOutputStreamOperator resultStream = resultStream = orderDataStream.connect(receipDataStream)
                .process(new OrderMatchFunction());

        resultStream.print("matched");
        resultStream.getSideOutput(unmatchedPayEventOutputTag).print("unmatched pays");
        resultStream.getSideOutput(unmatchedReceiptEventOutputTag).print("unmatched receipts");
        // ---------------------------------------------------------------------------------------

        resultStream.print();
        env.execute("tx match with join job");
    }

    /**
     * ԭʼ�汾1
     * */
    public static class OrderMatchWithJoinFunction extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>> {
        @Override
        public void processElement(OrderEvent orderEvent, ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            collector.collect(new Tuple2<>(orderEvent, receiptEvent));
        }
    }

    /**
     * �Ż��汾2
     * */
    public static class OrderMatchFunction extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        // ����״̬�����浱ǰ���׶�Ӧ�Ķ���֧���¼��͵����¼�
        transient ValueState<OrderEvent> payEventState = null;
        transient ValueState<ReceiptEvent> receiptEventState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            payEventState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay", OrderEvent.class));
            receiptEventState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt", TypeInformation.of(ReceiptEvent.class)));
        }

        @Override
        public void processElement1(OrderEvent orderEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            // ����֧�����ˣ�Ҫ�ж�֮ǰ�Ƿ��е����¼�
            ReceiptEvent receipt = receiptEventState.value();
            if( receipt != null ){
                // ����Ѿ���receipt���������ƥ�䣬���״̬
                collector.collect(new Tuple2(orderEvent, receipt));
                receiptEventState.clear();
                payEventState.clear();
            } else{
                // �����û����ע�ᶨʱ����ʼ�ȴ�5��
                context.timerService().registerEventTimeTimer(orderEvent.getTimestamp() * 1000L + 5000L);
                // ����״̬
                payEventState.update(orderEvent);
            }

        }

        @Override
        public void processElement2(ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            // �����¼����ˣ�Ҫ�ж�֮ǰ�Ƿ���pay�¼�
            OrderEvent pay = payEventState.value();
            if( pay != null ){
                // ����Ѿ���pay���������ƥ�䣬���״̬
                collector.collect(new Tuple2(pay, receiptEvent));
                receiptEventState.clear();
                payEventState.clear();
            } else{
                // �����û����ע�ᶨʱ����ʼ�ȴ�3��
                context.timerService().registerEventTimeTimer(receiptEvent.getTimestamp() * 1000L + 3000L);
                // ����״̬
                receiptEventState.update(receiptEvent);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            // ��ʱ���������ж�״̬���ĸ������ڣ��ʹ�����һ��û����������������
            if( payEventState.value() != null ){
                ctx.output(unmatchedPayEventOutputTag, payEventState.value());
            }
            if( receiptEventState.value() != null ){
                ctx.output(unmatchedReceiptEventOutputTag, receiptEventState.value());
            }
            // ���״̬
            receiptEventState.clear();
            payEventState.clear();

        }
    }

    public static class OrderEvent{
        private Long userId;
        private String action;
        private String orId;
        private Long timestamp;
    }

    public static class ReceiptEvent{
        private String orId;
        private String payEquipment;
        private Long timestamp;
    }

}
