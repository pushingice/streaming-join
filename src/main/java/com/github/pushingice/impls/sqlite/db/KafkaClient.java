package com.github.pushingice.impls.sqlite.db;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaClient {

    private static final Logger LOG = LoggerFactory.getLogger(
            KafkaClient.class.getCanonicalName());

    public static void main(String[] args) {


        Properties props = new Properties();
        try (InputStream input = new FileInputStream(args[0])) {
            props.load(input);
            LOG.info("{}:", args[0]);
            props.entrySet().forEach((x) ->
                    LOG.info("{}={}", x.getKey(), x.getValue()));
        } catch (FileNotFoundException e) {
            LOG.error("Couldn't find {}", args[0]);
        } catch (IOException e) {
            LOG.error("Problem reading {}", args[0]);
        }
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("group.id", "sqlite");
        kafkaConfig.setProperty("zookeeper.connect", "localhost:2181");
        Map<String, Integer> topicCountMap = new HashMap<>();
        ConsumerConnector consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(new ConsumerConfig(kafkaConfig));
        topicCountMap.put("message", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("message");
        KafkaStream<byte[], byte[]> messages = streams.get(0);
        messages.forEach(x -> LOG.info("{}", new String(x.message())));

    }
}
