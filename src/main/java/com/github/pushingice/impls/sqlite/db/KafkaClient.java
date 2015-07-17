package com.github.pushingice.impls.sqlite.db;

import com.github.pushingice.Message;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        final String columns = "(id integer primary key, " +
                "fk_id integer, fk_type string)";
        try (Connection connection = DriverManager
                .getConnection("jdbc:sqlite:message.db")) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("drop table if exists a");
            statement.executeUpdate("drop table if exists b");
            statement.executeUpdate("drop table if exists c");
            statement.executeUpdate("drop table if exists d");
            statement.executeUpdate("drop table if exists e");
            statement.executeUpdate("drop table if exists f");
            statement.executeUpdate("drop table if exists g");
            statement.executeUpdate("create table a " + columns);
            statement.executeUpdate("create table b " + columns);
            statement.executeUpdate("create table c " + columns);
            statement.executeUpdate("create table d " + columns);
            statement.executeUpdate("create table e " + columns);
            statement.executeUpdate("create table f " + columns);
            statement.executeUpdate("create table g " + columns);

            Properties kafkaConfig = new Properties();
            kafkaConfig.setProperty("group.id", "sqlite");
            kafkaConfig.setProperty("zookeeper.connect", "localhost:2181");
            Map<String, Integer> topicCountMap = new HashMap<>();
            ConsumerConnector consumer = kafka.consumer.Consumer
                    .createJavaConsumerConnector(
                            new ConsumerConfig(kafkaConfig));
            topicCountMap.put("message", 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                    consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams =
                    consumerMap.get("message");
            KafkaStream<byte[], byte[]> messages = streams.get(0);
            messages.forEach(x -> {
                String msgContent = new String(x.message());
                Message msg = gson.fromJson(msgContent, Message.class);
                LOG.info("{}", msg);
                try {
                    statement.executeUpdate(String.format(
                            "INSERT INTO %s VALUES (%d, %d, '%s')",
                            msg.getMessageType(), msg.getId(), msg.getFkId(),
                            msg.getFkMessageType()));
                } catch (SQLException e) {
                    LOG.error("{}", e.getMessage());
                    if (e.getMessage().startsWith("UNIQUE constraint")) {
                        try {
                            statement.executeUpdate(
                                    String.format("UPDATE %s SET " +
                                                    "fk_id = %d, fk_type='%s' " +
                                                    "WHERE id = %d ",
                                            msg.getMessageType(),
                                            msg.getFkId(),
                                            msg.getFkMessageType(),
                                            msg.getId())
                                    );
                        } catch (SQLException e1) {
                            LOG.error("{}", e1.getMessage());
                        }
                    }

                }
            });

        } catch (SQLException e) {
            LOG.error("{}", e.getMessage());
        }

    }
}
