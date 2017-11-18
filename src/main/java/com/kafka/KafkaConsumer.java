package com.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.kafka.util.KafkaTopic;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KafkaConsumer {

	private final ConsumerConnector consumer;

	private KafkaConsumer() {
		Properties props = new Properties();
		// zookeeper 配置
		props.put("zookeeper.connect", "nn01:2181,dn01:2181,dn02:2181");
		// 消费者所在组
		props.put("group.id", "testgroup");
		// zk连接超时
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ConsumerConfig config = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	}

	void consume() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(KafkaTopic.WEB_FROM_JT808, new Integer(1));
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,
				keyDecoder, valueDecoder);
		KafkaStream<String, String> stream = consumerMap.get(KafkaTopic.WEB_FROM_JT808).get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		while (it.hasNext()) {
			System.out.println("消费数据" + it.next().message());
		}
	}

	public static void main(String[] args) {
		new KafkaConsumer().consume();
	}
}