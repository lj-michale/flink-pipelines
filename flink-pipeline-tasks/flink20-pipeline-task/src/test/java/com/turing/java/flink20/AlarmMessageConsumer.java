//package com.turing.java.flink20;
//
//import com.turing.java.flink20.bean.AlarmMessage;
//import org.apache.pulsar.client.api.*;
//
///**
// * @descr AlarmMessageConsumer用于接收sink的内容
// * https://blog.csdn.net/Kartist139/article/details/137196549
// * */
//public class AlarmMessageConsumer {
//
//    static String serviceUrl = "pulsar://192.168.3.139:6650";
//    static String topic = "persistent://flink/demo1/alarm-message";
//
//    public static void main(String[] args) throws PulsarClientException {
//
//        PulsarClient pulsarClient = PulsarClient.builder()
//                .serviceUrl(serviceUrl)
//                .build();
//
//        System.out.println("start consumer");
//        Consumer<byte[]> subscribe = pulsarClient.newConsumer(Schema.BYTES)
//                .topic(topic)
//                .subscriptionName("sub-name")
//                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
//                .subscriptionType(SubscriptionType.Key_Shared)
//                .subscribe();
//
//        while (true) {
//            try {
//                Message<byte[]> message = subscribe.receive();
//                MessageId messageId = message.getMessageId();
//                AlarmMessage deserialize = ObjectUtil.deserialize(message.getValue(), AlarmMessage.class);
//                long timeMillis = System.currentTimeMillis();
//                System.out.println("delay:" + (timeMillis - deserialize.getTimestamp()) + " = " + timeMillis + "-" + deserialize.getTimestamp() + deserialize);
//
//                subscribe.acknowledge(messageId);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }
//}
//
