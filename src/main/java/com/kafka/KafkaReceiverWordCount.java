package com.kafka;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * 基于Kafka receiver方式的实时wordcount程序
 * 
 * @author Administrator
 *
 */
public class KafkaReceiverWordCount {
	public final static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("KafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

		// 使用KafkaUtils.createStream()方法，创建针对Kafka的输入数据流
		Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
		topicThreadMap.put("TOPIC_TEST", 3);

		JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, "127.0.0.1:2181",
				"DefaultConsumerGroup", topicThreadMap);

		// 然后开发wordcount逻辑
		JavaDStream<String> words = lines.flatMap(

				new FlatMapFunction<Tuple2<String, String>, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
						return Arrays.asList(tuple._2.split(" "));
					}

				});

		JavaPairDStream<String, Integer> pairs = words.mapToPair(

				new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String word) throws Exception {
						return new Tuple2<String, Integer>(word, 1);
					}

				});

		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(

				new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}

				});

		wordCounts.foreach(new Function<JavaPairRDD<String, Integer>, Void>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaPairRDD<String, Integer> v1) throws Exception {
				System.out.println(formatter.format(new Date()) + "-----接收数据-----" + v1.id() + "-->" + v1.name());
				return null;
			}
		});

		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}

}
