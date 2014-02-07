package com.guywald.storm.trident.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StormTridentKafkaTest {

	private static Logger log = LoggerFactory.getLogger(StormTridentKafkaTest.class);

	@SuppressWarnings("serial")
	public static class Printer extends BaseFunction {

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String msg = tuple.getString(0);
			log.info(msg);
			System.out.println(msg);
			collector.emit(new Values(msg));
		}

	}

	public static StormTopology buildTopology() {
		try {
			String zookeepers = "127.0.0.1:2181";
			BrokerHosts brokerHosts = new ZkHosts(zookeepers);
			TridentKafkaConfig config = new TridentKafkaConfig(brokerHosts,
					"test_topic");
			config.scheme = new SchemeAsMultiScheme(new StringScheme());

			TridentTopology topology = new TridentTopology();
			topology.newStream("spout1",
					new TransactionalTridentKafkaSpout(config)).each(
					new Fields("str"), new Printer(), new Fields("text"));

			return topology.build();
		} catch (Exception e) {
			System.err.println("Caught IOException: " + e.getMessage());
		}
		return null;
	}

	public static void main(String[] args) {
		log.info("Simple Trident Kafka spout test for Kafka version 0.8 and Storm 0.9 (Version in pom.xml)");
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", new Config(), buildTopology());
		while (true) {

		}
	}
}
