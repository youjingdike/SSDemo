package com.xq.tools;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaOffsetTool {

  private volatile static KafkaOffsetTool instance;
  final int TIMEOUT = 100000;
  final int BUFFERSIZE = 64 * 1024;

  private KafkaOffsetTool() {
  }

  public static KafkaOffsetTool getInstance() {
	  if (instance == null) {
		synchronized (KafkaOffsetTool.class) {
			if (instance == null) {
				instance = new KafkaOffsetTool();
			}
		}
	  }
	return instance;
  }

/**
 * 获取topic最后的偏移量(与分组无关)
 * @param brokerList
 * @param topics
 * @param clientId
 * @return
 */
public Map<TopicAndPartition, Long> getLastOffset(String brokerList, List<String> topics,
      String clientId) {

    Map<TopicAndPartition, Long> topicAndPartitionLongMap = Maps.newHashMap();

    Map<TopicAndPartition, BrokerEndPoint > topicAndPartitionBrokerMap =
        KafkaOffsetTool.getInstance().findLeader(brokerList, topics);

    for (Map.Entry<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerEntry : topicAndPartitionBrokerMap
        .entrySet()) {
      // get leader broker
    	BrokerEndPoint  leaderBroker = topicAndPartitionBrokerEntry.getValue();

      SimpleConsumer simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(),
          TIMEOUT, BUFFERSIZE, clientId);

      long readOffset = getTopicAndPartitionLastOffset(simpleConsumer,
          topicAndPartitionBrokerEntry.getKey(), clientId);

      topicAndPartitionLongMap.put(topicAndPartitionBrokerEntry.getKey(), readOffset);
      if (simpleConsumer!=null) {
    	  simpleConsumer.close();
      }
    }
    System.out.println("getLastOffset:"+topicAndPartitionLongMap);
    return topicAndPartitionLongMap;

  }

  /**
   * 获取topic最早的偏移量(与分组无关)
   * @param brokerList
   * @param topics
   * @param clientId
   * @return
   */
  public Map<TopicAndPartition, Long> getEarliestOffset(String brokerList, List<String> topics,
      String clientId) {

    Map<TopicAndPartition, Long> topicAndPartitionLongMap = Maps.newHashMap();

    Map<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerMap =
        KafkaOffsetTool.getInstance().findLeader(brokerList, topics);

    for (Map.Entry<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerEntry : topicAndPartitionBrokerMap
        .entrySet()) {
      // get leader broker
    	BrokerEndPoint leaderBroker = topicAndPartitionBrokerEntry.getValue();

      SimpleConsumer simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(),
          TIMEOUT, BUFFERSIZE, clientId);

      long readOffset = getTopicAndPartitionEarliestOffset(simpleConsumer,
          topicAndPartitionBrokerEntry.getKey(), clientId);

      topicAndPartitionLongMap.put(topicAndPartitionBrokerEntry.getKey(), readOffset);
      if (simpleConsumer!=null) {
    	  simpleConsumer.close();
      }
    }
    
    System.out.println("getEarliestOffset"+topicAndPartitionLongMap);

    return topicAndPartitionLongMap;

  }

  /**
   * 得到所有的 TopicAndPartition
   *
   * @param brokerList
   * @param topics
   * @return topicAndPartitions
   */
  public Map<TopicAndPartition, BrokerEndPoint> findLeader(String brokerList, List<String> topics) {
    // get broker's url array
    String[] brokerUrlArray = getBorkerUrlFromBrokerList(brokerList);
    // get broker's port map
    Map<String, Integer> brokerPortMap = getPortFromBrokerList(brokerList);

    // create array list of TopicAndPartition
    Map<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerMap = Maps.newHashMap();

    for (String broker : brokerUrlArray) {

      SimpleConsumer consumer = null;
      try {
        // new instance of simple Consumer
        consumer = new SimpleConsumer(broker, brokerPortMap.get(broker), TIMEOUT, BUFFERSIZE,
            "leaderLookup" + new Date().getTime());

        TopicMetadataRequest req = new TopicMetadataRequest(topics);

        TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> metaData = resp.topicsMetadata();

        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            TopicAndPartition topicAndPartition =
                new TopicAndPartition(item.topic(), part.partitionId());
            topicAndPartitionBrokerMap.put(topicAndPartition, part.leader());
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (consumer != null)
          consumer.close();
      }
    }
    System.out.println("findLeader:"+topicAndPartitionBrokerMap);
    return topicAndPartitionBrokerMap;
  }

  /**
   * get last offset
   * @param consumer
   * @param topicAndPartition
   * @param clientId
   * @return
   */
  private long getTopicAndPartitionLastOffset(SimpleConsumer consumer,
      TopicAndPartition topicAndPartition, String clientId) {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
        kafka.api.OffsetRequest.LatestTime(), 1));

    OffsetRequest request = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
        clientId);

    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      System.out
          .println("Error fetching data Offset Data the Broker. Reason: "
              + response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
      return 0;
    }
    long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
    System.out.println("getTopicAndPartitionLastOffset:"+offsets[0]);
    return offsets[0];
  }

  /**
   * get earliest offset
   * @param consumer
   * @param topicAndPartition
   * @param clientId
   * @return
   */
  private long getTopicAndPartitionEarliestOffset(SimpleConsumer consumer,
      TopicAndPartition topicAndPartition, String clientId) {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
        kafka.api.OffsetRequest.EarliestTime(), 1));

    OffsetRequest request = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
        clientId);

    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      System.out
          .println("Error fetching data Offset Data the Broker. Reason: "
              + response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
      return 0;
    }
    long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
    System.out.println("getTopicAndPartitionEarliestOffset:"+offsets[0]);
    return offsets[0];
  }
  
  /**
   * 得到所有的broker url,url之间用','隔开
   *
   * @param brokerlist
   * @return
   */
  private String[] getBorkerUrlFromBrokerList(String brokerlist) {
    String[] brokers = brokerlist.split(",");
    for (int i = 0; i < brokers.length; i++) {
      brokers[i] = brokers[i].split(":")[0];
    }
    return brokers;
  }

  /**
   * 得到broker url 与 其port 的映射关系
   *
   * @param brokerlist
   * @return
   */
  private Map<String, Integer> getPortFromBrokerList(String brokerlist) {
    Map<String, Integer> map = new HashMap<String, Integer>();
    String[] brokers = brokerlist.split(",");
    for (String item : brokers) {
      String[] itemArr = item.split(":");
      if (itemArr.length > 1) {
        map.put(itemArr[0], Integer.parseInt(itemArr[1]));
      }
    }
    return map;
  }

  public static void main(String[] args) {
    /*List<String> topics = Lists.newArrayList();
    topics.add("test");*/
	String brokerList = "pg253liulu:9092";
	String clientId = "offsetLookup" + new Date().getTime();
    List<String> topics = Arrays.asList("order1","order2","order3","orderprocess","kafka123");
	Map<TopicAndPartition, Long> topicAndPartitionLongMapEarliest =
        KafkaOffsetTool.getInstance().getEarliestOffset(brokerList, topics,
            clientId);

    Map<TopicAndPartition, Long> topicAndPartitionLongMapLast =
        KafkaOffsetTool.getInstance().getLastOffset(brokerList, topics,
            clientId);
    
    for (Map.Entry<TopicAndPartition, Long> entry : topicAndPartitionLongMapEarliest.entrySet()) {
    	System.out.println("!!!!!!!!!!!!Earliest:"+entry.getKey().topic() + "-"+ entry.getKey().partition() + ":" + entry.getValue());
    }
    
    for (Map.Entry<TopicAndPartition, Long> entry : topicAndPartitionLongMapLast.entrySet()) {
    	System.out.println("~~~~~~~~~~~~~Last:"+entry.getKey().topic() + "-"+ entry.getKey().partition() + ":" + entry.getValue());
    }
  }
}