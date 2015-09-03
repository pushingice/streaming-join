package com.github.pushingice.impls.sqlite.db;

import com.github.pushingice.Message;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.KafkaConsumerStreams;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class KafkaDBLoader {

    private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s";

    private static final String CREATE_TABLE_LINK = "CREATE TABLE %1$s_%2$s (" +
            "%1$s_id INTEGER REFERENCES %1$s(id), " +
            "%2$s_id INTEGER REFERENCES %2$s(id)," +
            "PRIMARY KEY (%1$s_id, %2$s_id))";

    private static final String CREATE_TABLE_BASE = "CREATE TABLE %s (" +
            "id INTEGER PRIMARY KEY," +
            "content TEXT)";


    private static final Logger LOG = LoggerFactory.getLogger(
            KafkaDBLoader.class.getSimpleName());

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
        try (Connection connection = DriverManager
                .getConnection("jdbc:sqlite:message.db")) {
            Statement statement = connection.createStatement();
            statement.executeUpdate(String.format(DROP_TABLE, "a"));
            statement.executeUpdate(String.format(DROP_TABLE, "b"));
            statement.executeUpdate(String.format(DROP_TABLE, "c"));
            statement.executeUpdate(String.format(DROP_TABLE, "d"));
            statement.executeUpdate(String.format(DROP_TABLE, "e"));
            statement.executeUpdate(String.format(DROP_TABLE, "f"));
            statement.executeUpdate(String.format(DROP_TABLE, "g"));
            statement.executeUpdate(String.format(DROP_TABLE, "a_b"));
            statement.executeUpdate(String.format(DROP_TABLE, "a_c"));
            statement.executeUpdate(String.format(DROP_TABLE, "b_d"));
            statement.executeUpdate(String.format(DROP_TABLE, "b_e"));
            statement.executeUpdate(String.format(DROP_TABLE, "d_f"));
            statement.executeUpdate(String.format(DROP_TABLE, "d_g"));
            statement.executeUpdate(String.format(CREATE_TABLE_BASE, "a"));
            statement.executeUpdate(String.format(CREATE_TABLE_BASE, "b"));
            statement.executeUpdate(String.format(CREATE_TABLE_BASE, "c"));
            statement.executeUpdate(String.format(CREATE_TABLE_BASE, "d"));
            statement.executeUpdate(String.format(CREATE_TABLE_BASE, "e"));
            statement.executeUpdate(String.format(CREATE_TABLE_BASE, "f"));
            statement.executeUpdate(String.format(CREATE_TABLE_BASE, "g"));
            statement.executeUpdate(String.format(CREATE_TABLE_LINK, "a", "b"));
            statement.executeUpdate(String.format(CREATE_TABLE_LINK, "a", "c"));
            statement.executeUpdate(String.format(CREATE_TABLE_LINK, "b", "d"));
            statement.executeUpdate(String.format(CREATE_TABLE_LINK, "b", "e"));
            statement.executeUpdate(String.format(CREATE_TABLE_LINK, "d", "f"));
            statement.executeUpdate(String.format(CREATE_TABLE_LINK, "d", "g"));

            KafkaStream<byte[], byte[]> messages =
                    KafkaConsumerStreams.getStreams("sqlite" +
                            Long.toString(System.currentTimeMillis()),
                            "localhost:2181", true, "message", 1).get(0);

            messages.forEach(x -> {
                String msgContent = new String(x.message());
                Message msg = gson.fromJson(msgContent, Message.class);
                LOG.info("{}", msg);
                try {
                    if (msg.getFkMessageType().isEmpty()) {
                        statement.executeUpdate(String.format(
                                "INSERT INTO %s VALUES (%d, '%s')",
                                msg.getMessageType(), msg.getId(),
                                msg.getContent()
                        ));
                    } else {
                        statement.executeUpdate(String.format(
                                "INSERT INTO %s_%s VALUES (%d, %d)",
                                msg.getMessageType(), msg.getFkMessageType(),
                                msg.getId(), msg.getFkId()
                        ));
                    }
                } catch (SQLException e) {
                    LOG.error("{}", e.getMessage());
                }
            });

        } catch (SQLException e) {
            LOG.error("{}", e.getMessage());
            LOG.error("{}", e.getStackTrace());
        }

    }
}
