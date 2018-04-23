package com.xq.test;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;



public class Streaming2KafkaExample {
	private static final int BATCH_INTERVAL_SECONDS = 1;

    private static final String TOPIC = "dataplatform_yarn_application_metrics";
    private static final String BOOTSTRAP_SERVERS = "kafka1.dns.guazi.com:9092,kafka2.dns.guazi.com:9092,kafka3.dns.guazi.com:9092,kafka4.dns.guazi.com:9092";
    private static final String GROUP_ID = "spark_streaming_test";

    private static final Properties KAFKA_CONSUMER_PARAMS = loadProperties();

    private static Properties loadProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    /*private static List<JavaInputDStream<String>> createDStreams(
            List<String> topics,
            KafkaConsumer<String, String> consumer,
            JavaStreamingContext jssc,
            Map<String, String> params) {
        return topics.stream().map(topic -> {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            consumer.assign(partitionInfos.stream()
                    .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                    .collect(Collectors.toSet()));
            Map<TopicAndPartition, Long> fromOffsets = consumer.assignment().stream()
                    .collect(Collectors.toMap(
                            topicPartition -> new TopicAndPartition(topicPartition.topic(), topicPartition.partition()),
                            consumer::position));
            return KafkaUtils.createDirectStream(
                    jssc,
                    String.class,
                    String.class,
                    StringDecoder.class,
                    StringDecoder.class,
                    String.class,
                    params,
                    fromOffsets,
                    (Function<MessageAndMetadata<String, String>, String>) MessageAndMetadata::message
            );
        }).collect(Collectors.toList());
    }*/

    private static SparkConf createSparkConf(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        conf.set("spark.streaming.kafka.maxRatePerPartition", "100");
        return conf;
    }

	private static Map<String, String> createSparkParams() {
        Map<String, String> params = new HashMap<>();
        params.put("metadata.broker.list", BOOTSTRAP_SERVERS);
        params.put("group.id", GROUP_ID);
        return params;
    }

    private static void reportOffsets(List<JavaInputDStream<String>> dStreams, KafkaConsumer<String, String> consumer) {
        dStreams.forEach(dStream -> dStream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            for (OffsetRange offsetRange : offsetRanges) {
                consumer.commitSync(Collections.singletonMap(
                        new TopicPartition(offsetRange.topic(), offsetRange.partition()),
                        new OffsetAndMetadata(offsetRange.untilOffset() + 1)));
            }

        }));
    }

    public static void main(String args[]) throws InterruptedException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KAFKA_CONSUMER_PARAMS);

        JavaStreamingContext jssc = new JavaStreamingContext(
                createSparkConf("KafkaTest"),
                Durations.seconds(BATCH_INTERVAL_SECONDS));

 /*       List<JavaInputDStream<String>> dStreams = createDStreams(
                Arrays.asList(TOPIC),
                consumer,
                jssc,
                createSparkParams());

        dStreams.forEach(dStream -> dStream.count().print());

        reportOffsets(dStreams, consumer);
*/
        jssc.start();
        jssc.awaitTermination();
    }

	
	
}
