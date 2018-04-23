package com.xq.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.xq.db.dao.PhoenixDao;

import scala.Tuple2;

public class WordCountOnLine {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("WordCountOnLine");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
		
		JavaReceiverInputDStream<String> linesDStram = streamingContext.socketTextStream("node5", 9999);
		
		JavaDStream<String> wordsDStream = linesDStram.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String str) throws Exception {
				return Arrays.asList(str.split(" ")).iterator();
			}
		});
		
		
		JavaPairDStream<String, Integer> pairDStram = wordsDStream.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t,1);
			}
		});
		
		JavaPairDStream<String, Integer> resultDStream = pairDStram.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		resultDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Integer> jpr) throws Exception {
				jpr.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Integer>> it) throws Exception {
						List<Object[]> insertParams = new ArrayList<Object[]>();
						
						while(it.hasNext()){
							Tuple2<String, Integer> next = it.next();
							insertParams.add(new Object[]{next._2,next._1});
						}
						System.out.println(insertParams);
						new PhoenixDao().doBatchVoid("upSERT INTO test VALUES(?,?)", insertParams);
					}
				});
			}
		});
		
		streamingContext.start();
		streamingContext.awaitTermination();
		
		streamingContext.close();
	}
}
