package com.xq.ss.demo;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.xq.db.dao.PgDao;
import com.xq.tools.ZKTools;
/**
 * 用途：测试通过正则构建需要监控的topic
 * @author xingqian
 *
 */
public class SaveOffset2ZKSSDemo2 implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws Exception {
//		Logger.getLogger("org.apache.spark").setLevel(Level.INFO);
		SparkConf conf = new SparkConf().setAppName("TestPattern");
		if (args.length > 0 && "spark".equalsIgnoreCase(args[0])) {
			conf.setMaster("spark://node5:7077");
		} else {
			conf.setMaster("local[1]");
		}
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "com.tzx.log.kryo.MyRegistrator");
		conf.set("spark.streaming.kafka.maxRatePerPartition", "20");
		conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
//		conf.set("spark.streaming.stopSparkContextByDefault", "false");
		// 根据sparkconf 创建JavaStreamingContext
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
//		jsc.checkpoint("hdfs://10.99.0.9:8020/user/xingqian/SparkStreaming/CheckPoint_wm_test");
		
		HashMap<String, Object> kafkaParameters = new HashMap<String, Object>();
//		kafkaParameters.put("metadata.broker.list", "pg253liulu:9092");
		kafkaParameters.put("key.deserializer", StringDeserializer.class);
		kafkaParameters.put("value.deserializer", StringDeserializer.class);
		kafkaParameters.put("bootstrap.servers", "node5:9092");
		kafkaParameters.put("group.id", "cscscscs222");
		kafkaParameters.put("auto.offset.reset", "latest");
//		kafkaParameters.put("zk.connect", KafkaProperties.zkConnect);
		kafkaParameters.put("zk.connect", "node5:2181");
		Broadcast<HashMap<String, Object>> broadcast = jsc.sparkContext().broadcast(kafkaParameters);
		
		/*
		 * 在这里可以从zk获取保存的偏移量，并对偏移量进行纠正，如果没有就初始化一个偏移量
		 * 因为kafka的数据会过期的，这样我们保存的偏移量的数据就有可能已经删掉了
		 */
		Collection<String> topics = Arrays.asList("test");
//		Collection<String> topics = Arrays.asList("kafka123");
//		Map<TopicPartition, Long> offsets = ZKTools.getOffsets(topics, kafkaParameters);
		
		/*为sparkStreaming设置读取topic-partition的偏移量*/
		/*List<Tuple3<String, Integer, Long>> list = new ArrayList<>();
//		list.add(new Tuple3<String, Integer, Long>("kafka123", 0, 39641982L));
		list.add(new Tuple3<String, Integer, Long>("kafka123", 0, 49626997L));
		Map<TopicPartition, Long> setFromOffsets = com.test.kafka.KafkaUtils.setFromOffsets(list);*/
		
		Pattern pattern = Pattern.compile("te.*", Pattern.CASE_INSENSITIVE);
		// 根据传入的参数：创建inputDStream(官方给的，如果我们自己保存偏移量)
		JavaInputDStream<ConsumerRecord<String, String>> dStream = KafkaUtils.createDirectStream(
				jsc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>SubscribePattern(pattern , kafkaParameters));
		/**
		 * 收集数据
		 */
		dStream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecord, Time time) throws Exception {
				final OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecord.rdd()).offsetRanges();
				SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date d = new Date(time.milliseconds());
				String tm = sim.format(d);
System.out.println("!!!!!!!!!!!!!!!!!!!!!!!rdd接收的数量:"+consumerRecord.count());
System.out.println("@@@@@@@@@@@@@@@@@@@job开始时间:"+tm);
consumerRecord.saveAsTextFile("d:\\logdata_wm\\2\\"+time.milliseconds());
				consumerRecord.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String,String>>>() {
					
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<ConsumerRecord<String, String>> iterator) throws Exception {
						while (iterator.hasNext()) {
							try {
								ConsumerRecord<String, String> conRec = iterator.next();
								System.out.println("topic:"+conRec.topic());
								
								String value = conRec.value();
								System.out.println("接收到的数据："+value);
								new PgDao().insertDB(conRec.topic()+":"+value);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
				        System.out.println("partition-offsets:"+
				          o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
				        ZKTools.getInstance().updateOffsets2ZK(o,broadcast.value());
					}
				});
				
				/*for (OffsetRange offsetRange : offsetRanges) {
					System.out.println(offsetRange.fromOffset()+";"+offsetRange.untilOffset()+";"+offsetRange.count()+";"+offsetRange.partition()+";"+offsetRange.topic());
				}*/
				
				//保存偏移量（也可以在partition处理完成后保存相应的偏移量）
//				ZKUtils.getInstance().updateOffsets2ZK(offsetRanges,kafkaParameters);
//				ZKUtils.getInstance().updateOffsets2ZKOld(offsets,kafkaParameters);
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
