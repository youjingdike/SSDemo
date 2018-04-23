package com.xq.tools;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

import kafka.common.TopicAndPartition;
import scala.Tuple3;

public class KafkaTools implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static class KafkaToolsHolder {  
	    private static final KafkaTools INSTANCE = new KafkaTools();  
    }
	private KafkaTools() {}
	
	public static KafkaTools getInstance() {
		return KafkaToolsHolder.INSTANCE;
	}
	
	/**
	 * 这个方法主要是为了保留这些参数,以及做测试用
	 * @return
	 */
	public <E, T> KafkaConsumer<E, T> getKafkaConsumer() {
		
		HashMap<String, Object> kafkaParameters = new HashMap<String, Object>();
//		kafkaParameters.put("metadata.broker.list", "pg253liulu:9092");
		kafkaParameters.put("key.deserializer", StringDeserializer.class);
		kafkaParameters.put("value.deserializer", StringDeserializer.class);
		kafkaParameters.put("bootstrap.servers", "node5:9092");
		kafkaParameters.put("group.id", "cscscscs4441");
//		kafkaParameters.put("enable.auto.commit", "false");
//		kafkaParameters.put("session.timeout.ms", "50000");
		//request.timeout.ms should be greater than session.timeout.ms and fetch.max.wait.ms
//		kafkaParameters.put("fetch.max.wait.ms", "50000");
//		kafkaParameters.put("request.timeout.ms", "60000");
		
		/*
		 * auto.offset.reset说明：(可以理解为初始化策略)
		 * 这个选项的作用是：某个新的分组如果在kafka里没有对应topic的便宜量信息，这时会按这个配置项的配置初始化一个初始偏移量；
		 * 即使是查询也会初始化这个偏移量，不管有没有提交，之后再查询，就查到的是初始化时的按配置策略初始化的值(这个造成了很长时间的疑惑)。
		 */
		
//		kafkaParameters.put("auto.offset.reset", "earliest");
		kafkaParameters.put("auto.offset.reset", "latest");
		
		KafkaConsumer<E, T> consumer = new KafkaConsumer<>(kafkaParameters);
		return consumer;
	}
	
	/**
	 * 提交该分组在相应的topic-partition上的偏移量
	 * @param consumer
	 * @param topic
	 * @param partition
	 * @param offsets
	 */
	public void commit(KafkaConsumer<?, ?> consumer,String topic,int partition,long offsets) {
		if(consumer!=null) consumer.commitSync(Collections.singletonMap(
                new TopicPartition(topic,partition),
                new OffsetAndMetadata(offsets)));
	}
	
	/**
	 * 获取传入的所有topic的所有partition信息
	 * @param consumer
	 * @param topics
	 * @return
	 */
	public List<TopicPartition> getTopicPartitionInfo(KafkaConsumer<?, ?> consumer,Collection<String> topics) {
		List<TopicPartition> list = new ArrayList<>();
		if(consumer==null) return list;
		for (String tp : topics) {
			Collection<PartitionInfo> partitionsFor = consumer.partitionsFor(tp);//获取传入的topic的partition的信息
			for (PartitionInfo p : partitionsFor) {
				list.add(new TopicPartition(tp, p.partition()));
			}
		}
		return list;
	}
	
	/**
	 * 获取当前分组在传入的所有topic的所有partition的偏移量
	 * @param consumer
	 * @param topics
	 * @return
	 */
	public Set<TopicPartition> getTopicPartitionOffsets(KafkaConsumer<?, ?> consumer,Collection<String> topics) {
		if (consumer!=null) {
			consumer.assign(getTopicPartitionInfo(consumer, topics));//分配给当前分组的consumer
			//获取当前分组在分配的TopicPartition的偏移量(注:想要这里面有值，必须执行assign方法)
			return consumer.assignment();//执行完该方法后，才能在consumer.position(topicPartition)这里获取到偏移量
		}
		return new HashSet<TopicPartition>();
	}
	
	/**
	 * 获取当前kafka的所有topic的信息
	 * @param consumer
	 * @param topics
	 * @return
	 */
	public Map<String, List<PartitionInfo>> getTopicList(KafkaConsumer<?, ?> consumer) {
		return consumer.listTopics();
	}
	
	/**
	 * 将list(topic,partition,offsets)的信息转换成Map<TopicPartition,Long>
	 * @param list
	 * @return
	 */
	public Map<TopicPartition,Long> setFromOffsets(List<Tuple3<String, Integer, Long>> list) {
		Map<TopicPartition, Long> fromOffsets = new HashMap<>();
		for (Tuple3<String, Integer, Long> t : list) {
			TopicPartition tp = new TopicPartition(t._1(),t._2());//topic和分区数
			fromOffsets.put(tp,t._3());           // offset位置
		}
		return fromOffsets;
	}
	
	
	public void reportOffsets(OffsetRange[] offsets, KafkaConsumer<String, String> consumer) {
		for (OffsetRange offsetRange : offsets) {
			System.out.println(offsetRange.fromOffset()+";"+offsetRange.untilOffset()+";"+offsetRange.count()+";"+offsetRange.partition()+";"+offsetRange.topic());
			
			/*
			 *  报错：
			 *  org.apache.kafka.clients.consumer.CommitFailedException: Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured session.timeout.ms, which typically implies that the poll loop is spending too much time message processing.
			 *   You can address this either by increasing the session timeout or by reducing the maximum size of batches returned in poll() with max.poll.records.
			 */
			
			consumer.commitSync(Collections.singletonMap(
					new TopicPartition(offsetRange.topic(), offsetRange.partition()),
					new OffsetAndMetadata(offsetRange.untilOffset() + 1)));
			consumer.close();
		}
			
	}
	
	public void reportOffsets(List<JavaInputDStream<String>> dStreams, KafkaConsumer<String, String> consumer) {
        dStreams.forEach(dStream -> dStream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            for (OffsetRange offsetRange : offsetRanges) {
                consumer.commitSync(Collections.singletonMap(
                        new TopicPartition(offsetRange.topic(), offsetRange.partition()),
                        new OffsetAndMetadata(offsetRange.untilOffset() + 1)));
            }

        }));
    }
	
	/**
	 * 矫正偏移量
	 * @param kafkaParams
	 * @param topics 
	 */
	public void rectifyOffsets(HashMap<String, Object> kafkaParams, Collection<String> topics) {
		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(kafkaParams);
		
		//获取topic最早的偏移量(与分组无关)
		Map<TopicAndPartition, Long> topicAndPartitionLongMapEarliest =
		        KafkaOffsetTool.getInstance().getEarliestOffset((String)kafkaParams.get("bootstrap.servers"), (List)topics,
		        		(String)kafkaParams.get("group.id"));
		//获取topic最早的偏移量(与分组无关)
	    Map<TopicAndPartition, Long> topicAndPartitionLongMapLast =
	            KafkaOffsetTool.getInstance().getLastOffset((String)kafkaParams.get("bootstrap.servers"), (List)topics,
		        		(String)kafkaParams.get("group.id"));
	    Set<TopicPartition> assignment = getTopicPartitionOffsets(consumer, topics);//获取当前分组在分配的TopicPartition的偏移量
System.out.println("assignment:"+assignment);
	    assignment.forEach(tp -> {
	    	long v = consumer.position(tp);
	    	if ((topicAndPartitionLongMapEarliest.get(new TopicAndPartition(tp.topic(), tp.partition()))!=null&&v < topicAndPartitionLongMapEarliest.get(new TopicAndPartition(tp.topic(), tp.partition()))) || (topicAndPartitionLongMapLast.get(new TopicAndPartition(tp.topic(), tp.partition()))!=null&&v > topicAndPartitionLongMapLast.get(new TopicAndPartition(tp.topic(), tp.partition())))) {
	    		if ("earliest".equals(kafkaParams.get("auto.offset.reset"))) {
	    			commit(consumer, tp.topic(), tp.partition(), topicAndPartitionLongMapEarliest.get(new TopicAndPartition(tp.topic(), tp.partition())));
	    		} else {
	    			commit(consumer, tp.topic(), tp.partition(), topicAndPartitionLongMapLast.get(new TopicAndPartition(tp.topic(), tp.partition())));
	    		}
	    	}
	    });
		
		consumer.close();
	}
	
	public static void main(String[] args) {
//		KafkaConsumer<String, String> consumer = KafkaTools.<String,String>getKafkaConsumer();
//		Collection<String> topics = Arrays.asList("test");
		
		HashMap<String, Object> kafkaParameters = new HashMap<String, Object>();
		kafkaParameters.put("key.deserializer", StringDeserializer.class);
		kafkaParameters.put("value.deserializer", StringDeserializer.class);
		kafkaParameters.put("bootstrap.servers", "pg253liulu:9092");
		kafkaParameters.put("group.id", "wm-consumer-group");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaParameters);
		Collection<String> topics = Arrays.asList("order1","order2","order3","orderprocess","kafka123");
		
		/*提交某个topic的某个partion的对应分组的偏移量*/
//		KafkaTools.commit(consumer, "test", 0, 120);
		
		/*获取所有topic的所有partition*/
//		List<TopicPartition> list = KafkaUtils.getTopicPartitionInfo(consumer, topics);
		
		//获取当前kafka中所有的topic
		/*KafkaUtils.getTopicList(consumer).entrySet().forEach(tp -> {
			System.out.println("~~~~~~"+tp.getKey()+":"+tp.getValue());
		});*/
		
		Set<TopicPartition> assignment = KafkaTools.getInstance().getTopicPartitionOffsets(consumer, topics);//获取当前分组在分配的TopicPartition的偏移量
		for (TopicPartition topicPartition : assignment) {
			System.out.println("......"+topicPartition.topic()+":"+topicPartition.partition()+":"+consumer.position(topicPartition));
		}
		
		consumer.close();
	}

}
