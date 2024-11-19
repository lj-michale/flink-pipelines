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
 * 多流对账功能，整体来看也是挺简单的，主要用到的知识点是 Watermark，状态，测流，流合并 等
 * */
public class DuiZhangshanPipeline {

    private static final OutputTag unmatchedPayEventOutputTag = new OutputTag<OrderEvent>("unmatched-pay"){};
    private static final OutputTag unmatchedReceiptEventOutputTag = new OutputTag<ReceiptEvent>("unmatched-receipt"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        // 1. 读取订单事件数据
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

        // 2. 读取到账事件数据
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

        // -------------------------------关联处理-------------------------------------------------
//        DataStream resultStream = orderDataStream.intervalJoin(receipDataStream)  //这里使用相对关联
//                .between(Time.seconds(-3), Time.seconds(5))  // 订单数据等待到账数据时间前三秒到后三秒区间
//                .process(new OrderMatchWithJoinFunction());  // 自定义类输出服务上边条件的数据

        //合并两条流，进行处理
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
     * 原始版本1
     * */
    public static class OrderMatchWithJoinFunction extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>> {
        @Override
        public void processElement(OrderEvent orderEvent, ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            collector.collect(new Tuple2<>(orderEvent, receiptEvent));
        }
    }

    /**
     * 优化版本2
     * */
    public static class OrderMatchFunction extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        // 定义状态，保存当前交易对应的订单支付事件和到账事件
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
            // 订单支付来了，要判断之前是否有到账事件
            ReceiptEvent receipt = receiptEventState.value();
            if( receipt != null ){
                // 如果已经有receipt，正常输出匹配，清空状态
                collector.collect(new Tuple2(orderEvent, receipt));
                receiptEventState.clear();
                payEventState.clear();
            } else{
                // 如果还没来，注册定时器开始等待5秒
                context.timerService().registerEventTimeTimer(orderEvent.getTimestamp() * 1000L + 5000L);
                // 更新状态
                payEventState.update(orderEvent);
            }

        }

        @Override
        public void processElement2(ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            // 到账事件来了，要判断之前是否有pay事件
            OrderEvent pay = payEventState.value();
            if( pay != null ){
                // 如果已经有pay，正常输出匹配，清空状态
                collector.collect(new Tuple2(pay, receiptEvent));
                receiptEventState.clear();
                payEventState.clear();
            } else{
                // 如果还没来，注册定时器开始等待3秒
                context.timerService().registerEventTimeTimer(receiptEvent.getTimestamp() * 1000L + 3000L);
                // 更新状态
                receiptEventState.update(receiptEvent);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            // 定时器触发，判断状态中哪个还存在，就代表另一个没来，输出到侧输出流
            if( payEventState.value() != null ){
                ctx.output(unmatchedPayEventOutputTag, payEventState.value());
            }
            if( receiptEventState.value() != null ){
                ctx.output(unmatchedReceiptEventOutputTag, receiptEventState.value());
            }
            // 清空状态
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
