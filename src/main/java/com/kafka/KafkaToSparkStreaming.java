package com.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * 将kafka数据导入到hive表中
 * 
 * @author jakce
 *
 */
public class KafkaToSparkStreaming {

	protected static Log log = LogFactory.getLog(KafkaToSparkStreaming.class);

	protected static String hdfs_uri = "hdfs://192.168.0.224:8020";
	protected static String broker_list = "192.168.0.221:9092,192.168.0.222:9092,192.168.0.223:9092";

	public static void main(String[] args) throws Exception {

		/*
		 * if (args.length < 3) { System.err.
		 * println("Usage: KafkaToSparkStreaming <hdfs_uri> <broker_list> <topic1,topic2>"
		 * ); System.exit(1); } String hdfs_uri = args[0]; String broker_list =
		 * args[1]; String topic = args[2];
		 */

		log.warn("启动接收Kafka数据测试程序");

		SparkConf sparkConf = new SparkConf().setAppName("KafkaToSparkStreaming");
		/*
		 * if (args.length > 0 && "local".equals(args[0])) {
		 * sparkConf.setMaster("local[*]"); }
		 */
		// sparkConf.set("spark.streaming.backpressure.enabled", "true");
		// sparkConf.set("spark.sql.parquet.compression.codec", "snappy");
		// sparkConf.set("spark.sql.parquet.mergeSchema", "true");
		// sparkConf.set("spark.sql.parquet.binaryAsString", "true");

		final String checkpointDir = hdfs_uri + "/tmp/streaming_checkpoint";

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		// final HiveContext sqlContext = new HiveContext(jssc.sparkContext());
		jssc.checkpoint(checkpointDir);

		// 构建kafka参数map
		// 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", broker_list);
		kafkaParams.put("group.id", "test_group1");
		kafkaParams.put("auto.offset.reset", "smallest");
		// kafkaParams.put("kafka.ofset.reset","0");

		// 构建topic set
		Set<String> topics = new HashSet<String>();
		topics.add("test_topic");

		// 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
		// 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
		JavaPairInputDStream<String, String> realTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		// 将一条日志拆分诚多条（分隔符为";"）
		JavaDStream<String> logDStream = realTimeLogDStream
				.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = -4327021536484339309L;

					@Override
					public Iterable<String> call(Tuple2<String, String> tuple2) throws Exception {
						return Arrays.asList(tuple2._2.split(";"));
					}

				});

		log.warn("---数据保存至HDFS---");
		// logDStream.print();
		logDStream.dstream().saveAsTextFiles(hdfs_uri + "/tmp/data/kafka/", "kafkaData");

		jssc.start();
		jssc.awaitTermination();
	}

}
