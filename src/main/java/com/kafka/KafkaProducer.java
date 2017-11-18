package com.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	final static Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
	private final Producer<String, String> producer;
	public final static String TOPIC = "TOPIC_TEST";

	public final static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private KafkaProducer() {

		Properties props = new Properties();

		// 此处配置的是kafka的broker地址:端口列表
		props.put("metadata.broker.list", "127.0.0.1:9092");

		// 配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		// 配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");

		// request.required.acks
		// 0, which means that the producer never waits for an acknowledgement
		// from the broker (the same behavior as 0.7). This option provides the
		// lowest latency but the weakest durability guarantees (some data will
		// be lost when a server fails).
		// 1, which means that the producer gets an acknowledgement after the
		// leader replica has received the data. This option provides better
		// durability as the client waits until the server acknowledges the
		// request as successful (only messages that were written to the
		// now-dead leader but not yet replicated will be lost).
		// -1, which means that the producer gets an acknowledgement after all
		// in-sync replicas have received the data. This option provides the
		// best durability, we guarantee that no messages will be lost as long
		// as at least one in sync replica remains.
		props.put("request.required.acks", "-1");

		producer = new Producer<String, String>(new ProducerConfig(props));
	}

	void produce() throws Exception {
		int messageNo = 1;
		while (true) {
			String key = String.valueOf(messageNo);
			String data = "130,1,2,3,2,4,123.2123,123.2124,123.2125,123.2126,123.2127";
			producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
			System.out.println(formatter.format(new Date()) + "-----发送数据-----" + data);
			messageNo++;
			Thread.sleep(3000);
		}
	}

	public static void main(String[] args) {
		try {
			new KafkaProducer().produce();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}