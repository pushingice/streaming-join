package util;


import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerStreams {

    public static List<KafkaStream<byte[], byte[]>> getStreams(
            String groupId, String ZKHostAndPort, boolean fromBeginning,
            String topic, int numStreams) {

        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("group.id", groupId);
        kafkaConfig.setProperty("zookeeper.connect", ZKHostAndPort);
        if (fromBeginning) {
            kafkaConfig.setProperty("auto.offset.reset", "smallest");
        }
        Map<String, Integer> topicCountMap = new HashMap<>();
        ConsumerConnector consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(
                        new ConsumerConfig(kafkaConfig));
        topicCountMap.put(topic, numStreams);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumer.createMessageStreams(topicCountMap);

        return consumerMap.get(topic);
    }

}
