package com.xq.constant;

public interface KafkaProperties {

	String zkConnect1 = "node5:2181";
	String zkConnect = "pg253liulu:2181";
	String groupId1 = "test-consumer-group";
	String groupId_wm = "wm-consumer-group";
	String groupId_pay = "test-consumer-group";
	String groupId_test = "xq-consumer-group";
	String topic = "andy";
	String kafkaServerURL = "node5";
	int kafkaServerPort = 9092;
	int kafkaProducerBufferSize = 64 * 1024;
	int connectionTimeOut = 20000;
	int reconnectInterval = 10000;
	String clientId = "SimpleConsumerDemoClient";
}
