package com.xq.ss.demo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.xq.db.dao.PgDao;
import com.xq.db.dao.PhoenixDao;
import com.xq.db.util.ConnectPool;
import com.xq.db.util.DBUtil;
import com.xq.tools.KafkaTools;

/**
 * 用途：保存偏移量到topic
 * @author xingqian
 *
 */
public class SaveOffset2KafkaSSDemo implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws Exception {
//		Logger.getLogger("org.apache.spark").setLevel(Level.INFO);
		SparkConf conf = new SparkConf().setAppName("TestOffset2Topic");
		if (args.length > 0 && "spark".equalsIgnoreCase(args[0])) {
			conf.setMaster("spark://node5:7077");
		} else {
			conf.setMaster("local[1]");
		}
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "com.xq.ss.kryo.MyRegistrator");
		conf.set("spark.streaming.kafka.maxRatePerPartition", "10");
		conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
		// 根据sparkconf 创建JavaStreamingContext
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
//		jsc.checkpoint("hdfs://10.99.0.9:8020/user/xingqian/SparkStreaming/CheckPoint_wm_test");
		
		HashMap<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("bootstrap.servers", "node5:9092");
		kafkaParams.put("group.id", "xq-test-ph-topic");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		Collection<String> topics = Arrays.asList("test");
		
		//矫正偏移量
		KafkaTools.getInstance().rectifyOffsets(kafkaParams,topics);
		
		// 根据传入的参数：创建inputDStream
		final JavaInputDStream<ConsumerRecord<String, String>> dStream = KafkaUtils.createDirectStream(jsc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		
		Broadcast<JavaInputDStream<ConsumerRecord<String, String>>> broadcast = jsc.sparkContext().broadcast(dStream);
		/**
		 * 收集数据
		 */
		dStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {
				OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
System.out.println("!!!!!!!!!!!!!!!!!!!!!!!接收的数量:"+rdd.count());
				rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String,String>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<ConsumerRecord<String, String>> iterator) throws Exception {
						
						while (iterator.hasNext()) {
							try {
								ConsumerRecord<String, String> conRec = iterator.next();
								System.out.println("topic:"+conRec.topic());
								
								String value = conRec.value();
								System.out.println("接收到的数据："+value);
								new PhoenixDao().insertDB(DBUtil.getUUID(),value);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				});
				
				for (OffsetRange offsetRange : offsetRanges) {
					System.out.println("@@@@@:"+offsetRange.fromOffset()+";"+offsetRange.untilOffset()+";"+offsetRange.count()+";"+offsetRange.partition()+";"+offsetRange.topic());
				}
				System.out.println("save start");
				((CanCommitOffsets) broadcast.value().inputDStream()).commitAsync(offsetRanges);
				System.out.println("save end");
				for (OffsetRange offsetRange : offsetRanges) {
					System.out.println(".....:"+offsetRange.fromOffset()+";"+offsetRange.untilOffset()+";"+offsetRange.count()+";"+offsetRange.partition()+";"+offsetRange.topic());
				}
//				Thread.sleep(1000*10);
			}
		});

		// 6. 启动执行
		jsc.start();
		// 7. 等待执行停止，如有异常直接抛出并关闭
		jsc.awaitTermination();
		// 关闭
		jsc.close();
		
	}
	
}
