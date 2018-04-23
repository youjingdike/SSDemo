package com.xq.tools;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.xq.ss.seria.MyZkSerializer;

import kafka.cluster.BrokerEndPoint;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKGroupTopicDirs;

public class ZKTools implements Serializable {
	private static final long serialVersionUID = 1L;
	static final int TIMEOUT = 100000;
	static final int BUFFERSIZE = 64 * 1024;
	private static class ZKToolsHolder {  
	    private static final ZKTools INSTANCE = new ZKTools();  
    }
	private ZKTools() {}
	
	public static ZKTools getInstance() {
		return ZKToolsHolder.INSTANCE;
	}
	
	public static void main(String[] args) throws Exception {
		
		/*HashMap<String, Object> kafkaParameters = new HashMap<String, Object>();
		kafkaParameters.put("metadata.broker.list", "pg253liulu:9092");
		kafkaParameters.put("key.deserializer", StringDeserializer.class);
		kafkaParameters.put("value.deserializer", StringDeserializer.class);
		kafkaParameters.put("bootstrap.servers", "pg253liulu:9092");
		kafkaParameters.put("group.id", "cscscscs444");
		kafkaParameters.put("auto.offset.reset", "earliest");
//		kafkaParameters.put("auto.offset.reset", "latest");
		kafkaParameters.put("zk.connect", KafkaProperties.zkConnect);
		
		Collection<String> topics = Arrays.asList("kafka123");
		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(kafkaParameters);
		Map<TopicPartition, Long> offsets = getOffsets(topics, kafkaParameters, consumer);
System.out.println(offsets);
		consumer.close();*/
		
//		getOffsets2ZKTest();
		ZKTools.getInstance().updateOffsets2ZKTest();
	}
	
	private Map<TopicAndPartition, Long> getOffsetsFromZK(List<TopicPartition> topics,Map<String,Object> param) {
		if(topics==null||topics.isEmpty()) return null;
		
		ZkClient zk = new ZkClient((String) param.get("zk.connect"));
		zk.setZkSerializer(new MyZkSerializer());
		
		Map<TopicAndPartition, Long> res = new HashMap<>();
		
		topics.forEach(tp -> {
			String path = "/consumers/"+param.get("group.id")+"/offsets/"+tp.topic()+"/"+tp.partition();
System.out.println("zk:topic:"+tp.topic()+",partition:"+tp.partition()+",offset:"+(zk.<String>readData(path,true)==null?null:Long.parseLong(zk.<String>readData(path,true))));
			res.put(new TopicAndPartition(tp.topic(), tp.partition()), zk.<String>readData(path,true)==null?null:Long.parseLong(zk.<String>readData(path,true)));
		});
		if (zk!=null) {
			zk.close();
		}
		return res;
	}
	
	/**
	 * 获取list(topics)的偏移量信息
	 * @param list
	 * @return
	 */
	public Map<TopicPartition,Long> getOffsets(Collection<String> topics,Map<String,Object> params) {
		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(params);
		Map<TopicPartition,Long> res = null;
		//获取所有的partition信息
	    List<TopicPartition> topicPartitionInfo = KafkaTools.getInstance().getTopicPartitionInfo(consumer, topics);
	    Map<TopicAndPartition, Long> fromOffsets = getOffsetsFromZK(topicPartitionInfo, params);
	    
	    if(fromOffsets!=null) {
	    	res = rectifyOffsets(fromOffsets,topics, params, consumer);
	    }
	    consumer.close();
	    
		return res==null?new HashMap<>():res;
	}
	
	/**
	 * 纠正偏移量
	 * @param fromOffsets
	 * @param topicPartitionInfo
	 * @param topics
	 * @param params
	 * @param consumer
	 */
	private Map<TopicPartition, Long> rectifyOffsets(Map<TopicAndPartition, Long> fromOffsets,Collection<String> topics,Map<String,Object> params,KafkaConsumer<String,String> consumer) {
		Map<TopicAndPartition, Long> topicAndPartitionLongMapEarliest =
		        KafkaOffsetTool.getInstance().getEarliestOffset((String)params.get("bootstrap.servers"), (List)topics,
		        		(String)params.get("group.id"));
	    Map<TopicAndPartition, Long> topicAndPartitionLongMapLast =
	            KafkaOffsetTool.getInstance().getLastOffset((String)params.get("bootstrap.servers"), (List)topics,
		        		(String)params.get("group.id"));
	    Map<TopicPartition, Long> res = new HashMap<>();
	    fromOffsets.forEach((k,v) -> {
	    	if (v==null || v < topicAndPartitionLongMapEarliest.get(k) || v > topicAndPartitionLongMapLast.get(k)) {
	    		if ("earliest".equals(params.get("auto.offset.reset"))) {
	    			res.put(new TopicPartition(k.topic(), k.partition()), topicAndPartitionLongMapEarliest.get(k));
	    		} else {
	    			res.put(new TopicPartition(k.topic(), k.partition()), topicAndPartitionLongMapLast.get(k));
	    		}
	    	} else {
	    		res.put(new TopicPartition(k.topic(), k.partition()), v);
	    	}
	    });
	    return res;
	}
	
	/**
	 * 更新zk中的偏移量
	 * 可根据实际情况,将某些参数做为方法的参数传入
	 * @param offsets
	 */
	public void updateOffsets2ZK(OffsetRange[] offsets,Map<String,Object> params) {
		long commitTimestamp = System.currentTimeMillis();
		long expireTimestamp = Long.MAX_VALUE;
		String groupId = (String) params.get("group.id");
		int correlationId = 1;
		String clientId = "upOffset";
/*		SimpleConsumer consumer = new SimpleConsumer("pg253liulu", 9092, TIMEOUT, BUFFERSIZE,
				"leaderLookup" + new Date().getTime());
*/		
		SimpleConsumer consumer = null;
		
		OffsetMetadata offsetMetadata = null;
		Map<TopicAndPartition, OffsetAndMetadata> requestInfo = null;
		kafka.javaapi.OffsetCommitRequest oscReq = null;
		for (OffsetRange offsetRange : offsets) {
System.out.println(offsetRange.fromOffset()+";"+offsetRange.untilOffset()+";"+offsetRange.count()+";"+offsetRange.partition()+";"+offsetRange.topic());
			if (offsetRange.count() != 0L) {
				Map<TopicAndPartition, BrokerEndPoint> leaders = KafkaOffsetTool.getInstance().findLeader((String)params.get("bootstrap.servers"), Collections.singletonList(offsetRange.topic()));
				for (Map.Entry<TopicAndPartition, BrokerEndPoint> leadersEntry : leaders.entrySet()) {
					if (offsetRange.topic().equals(leadersEntry.getKey().topic()) && offsetRange.partition()==leadersEntry.getKey().partition()){
						consumer = new SimpleConsumer(leadersEntry.getValue().host(), leadersEntry.getValue().port(), TIMEOUT, BUFFERSIZE,
								"leaderLookup" + new Date().getTime());
						offsetMetadata = new OffsetMetadata(offsetRange.untilOffset(), "offset");
						requestInfo = Collections.singletonMap(
								new TopicAndPartition(offsetRange.topic(),offsetRange.partition()),new OffsetAndMetadata(offsetMetadata, commitTimestamp, expireTimestamp));
						oscReq = new kafka.javaapi.OffsetCommitRequest(groupId, requestInfo, correlationId, clientId + new Date().getTime());
						consumer.commitOffsets(oscReq);
						consumer.close();
						break;
					};
				}
			}
		}
		System.out.println("保存偏移量用时："+(System.currentTimeMillis()-commitTimestamp)+"ms");
	}
	
	/**
	 * 更新zk中的偏移量
	 * 可根据实际情况,将某些参数做为方法的参数传入
	 * @param offsetRange
	 */
	public void updateOffsets2ZK(OffsetRange offsetRange,Map<String,Object> params) {
		long commitTimestamp = System.currentTimeMillis();
		long expireTimestamp = Long.MAX_VALUE;
		String groupId = (String) params.get("group.id");
		int correlationId = 1;
		String clientId = "upOffset";
		/*		SimpleConsumer consumer = new SimpleConsumer("pg253liulu", 9092, TIMEOUT, BUFFERSIZE,
				"leaderLookup" + new Date().getTime());
		 */		
		SimpleConsumer consumer = null;
		
		OffsetMetadata offsetMetadata = null;
		Map<TopicAndPartition, OffsetAndMetadata> requestInfo = null;
		kafka.javaapi.OffsetCommitRequest oscReq = null;
System.out.println(offsetRange.fromOffset()+";"+offsetRange.untilOffset()+";"+offsetRange.count()+";"+offsetRange.partition()+";"+offsetRange.topic());
		if (offsetRange.count() != 0L) {
			Map<TopicAndPartition, BrokerEndPoint> leaders = KafkaOffsetTool.getInstance().findLeader((String)params.get("bootstrap.servers"), Collections.singletonList(offsetRange.topic()));
			for (Map.Entry<TopicAndPartition, BrokerEndPoint> leadersEntry : leaders.entrySet()) {
				if (offsetRange.topic().equals(leadersEntry.getKey().topic()) && offsetRange.partition()==leadersEntry.getKey().partition()){
					consumer = new SimpleConsumer(leadersEntry.getValue().host(), leadersEntry.getValue().port(), TIMEOUT, BUFFERSIZE,
							"leaderLookup" + new Date().getTime());
					offsetMetadata = new OffsetMetadata(offsetRange.untilOffset(), "offset");
					requestInfo = Collections.singletonMap(
							new TopicAndPartition(offsetRange.topic(),offsetRange.partition()),new OffsetAndMetadata(offsetMetadata, commitTimestamp, expireTimestamp));
					oscReq = new kafka.javaapi.OffsetCommitRequest(groupId, requestInfo, correlationId, clientId + new Date().getTime());
					consumer.commitOffsets(oscReq);
					consumer.close();
					break;
				};
			}
		}
		System.out.println("保存偏移量用时："+(System.currentTimeMillis()-commitTimestamp)+"ms");
	}
	
	/**
	 * 更新zk中的偏移量
	 * 可根据实际情况,将某些参数做为方法的参数传入
	 * @param offsets
	 */
	public void updateOffsets2ZKOld(OffsetRange[] offsets,Map<String,Object> params) {
		long commitTimestamp = System.currentTimeMillis();
		long expireTimestamp = Long.MAX_VALUE;
		String groupId = (String) params.get("group.id");
		int correlationId = 1;
		String clientId = "upOffset";
		SimpleConsumer consumer = new SimpleConsumer("pg253liulu", 9092, TIMEOUT, BUFFERSIZE,
				"leaderLookup" + new Date().getTime());
		
		OffsetMetadata offsetMetadata = null;
		Map<TopicAndPartition, OffsetAndMetadata> requestInfo = null;
		kafka.javaapi.OffsetCommitRequest oscReq = null;
		for (OffsetRange offsetRange : offsets) {
System.out.println(offsetRange.fromOffset()+";"+offsetRange.untilOffset()+";"+offsetRange.count()+";"+offsetRange.partition()+";"+offsetRange.topic());
			
			offsetMetadata = new OffsetMetadata(offsetRange.untilOffset() + 1, "offset");
			requestInfo = Collections.singletonMap(
					new TopicAndPartition(offsetRange.topic(),offsetRange.partition()),new OffsetAndMetadata(offsetMetadata, commitTimestamp, expireTimestamp));
			oscReq = new kafka.javaapi.OffsetCommitRequest(groupId, requestInfo, correlationId, clientId + new Date().getTime());
			consumer.commitOffsets(oscReq);
		}
		System.out.println("保存偏移量用时："+(System.currentTimeMillis()-commitTimestamp)+"ms");
	}
	
	/**
	 *  从ZK中获取偏移量
	 * 可根据实际情况,将某些参数做为方法的参数传入，并返回相应的信息
	 */
	public void getOffsets2ZKTest() {
		
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs("cscscscs222", "kafka123");
		
		ZkClient zk = new ZkClient("pg253liulu:2181");
		zk.setZkSerializer(new MyZkSerializer());
System.out.println(topicDirs.consumerOffsetDir());
System.out.println(topicDirs.consumerGroupOffsetsDir());
		List<String> children = zk.getChildren(topicDirs.consumerOffsetDir());
System.out.println(children);
		for (String path : children) {
System.out.println(zk.<String>readData(topicDirs.consumerOffsetDir()+"/"+path,true));
		}
//		String readData = zk.<String>readData(topicDirs.consumerOffsetDir()+"/0");
//		System.out.println(readData);
	}
	
	/**
	 * 更新zk中的偏移量
	 * 可根据实际情况,将某些参数做为方法的参数传入
	 * @param offsets
	 */
	public void updateOffsets2ZKTest() {
		OffsetMetadata offsetMetadata = new OffsetMetadata(5246187L, "offset");
		long commitTimestamp = System.currentTimeMillis();
		long expireTimestamp = Long.MAX_VALUE;
		Map<TopicAndPartition, OffsetAndMetadata> requestInfo = Collections.singletonMap(
				new TopicAndPartition("kafka123",0),new OffsetAndMetadata(offsetMetadata, commitTimestamp, expireTimestamp));
		String groupId = "cscscscs444";
		int correlationId = 1;
		String clientId = "2";
		kafka.javaapi.OffsetCommitRequest os = new kafka.javaapi.OffsetCommitRequest(groupId, requestInfo, correlationId, clientId);
		SimpleConsumer consumer = null;
        consumer = new SimpleConsumer("pg253liulu", 9093, TIMEOUT, BUFFERSIZE,
            "leaderLookup" + new Date().getTime());
        consumer.commitOffsets(os);
	}
}
