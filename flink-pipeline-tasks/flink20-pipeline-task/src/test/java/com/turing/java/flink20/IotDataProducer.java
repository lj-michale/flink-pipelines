//package com.turing.java.flink20;
//
//import com.turing.java.flink20.bean.IotData;
//import org.apache.commons.lang3.RandomUtils;
//import org.apache.flink.shaded.zookeeper3.io.netty.util.internal.ObjectUtil;
//import org.apache.pulsar.client.api.Producer;
//import org.apache.pulsar.client.api.PulsarClient;
//import org.apache.pulsar.client.api.Schema;
//
//import java.util.Map;
//
///**
// * @descr 生产者IotDataProducer,用于将消息生产到puslar中
// *
// * */
//public class IotDataProducer {
//    static String serviceUrl = "pulsar://192.168.3.139:6650";
//    static String topic = "persistent://flink/demo1/iot-source";
//
//    public static void main(String[] args) throws Exception {
//
//        PulsarClient pulsarClient = PulsarClient.builder()
//                .serviceUrl(serviceUrl)
//                .build();
//
//
//        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
//                .topic(topic)
//                .create();
//
//        while (true) {
//            long ts = System.currentTimeMillis();
//            IotData iotData = new IotData();
//            iotData.setTimestamp(ts);
//            iotData.setDeviceId(1);
//
//            Map<String, Double> data = new HashMap<>();
//            // 模拟数据的变化
//            data.put("var1", RandomUtils.nextDouble());
//            data.put("var2", RandomUtils.nextDouble());
//            iotData.setData(data);
//            producer.newMessage(Schema.BYTES)
//                    .key("0")
//                    .value(ObjectUtil.serialize(iotData))
//                    .send();
//            System.out.println(ts + " send iot data " + iotData);
//
//            // 下一秒 0 毫秒的时间
//            long timeMillis = System.currentTimeMillis();
//            long nextMills = timeMillis - (timeMillis % 1000) + 1000;
//            long sleepMills = nextMills - timeMillis;
//            if (sleepMills > 0) {
//                Thread.sleep(sleepMills);
//            }
//        }
//    }
//}
//
