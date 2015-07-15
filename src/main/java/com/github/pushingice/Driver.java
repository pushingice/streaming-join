package com.github.pushingice;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class Driver {

    private static final Logger LOG = LoggerFactory.getLogger(
            Driver.class.getCanonicalName());

    public static void main(String[] args) throws UnsupportedEncodingException {

        if (args.length < 1) {
            System.out.println("Please specify properties file.");
            System.exit(0);
        }

        Properties config = new Properties();

        try (InputStream input = new FileInputStream(args[0])) {
            config.load(input);
            LOG.info("{}:", args[0]);
            config.entrySet().forEach((x) ->
                    LOG.info("{}={}", x.getKey(), x.getValue()));
        } catch (FileNotFoundException e) {
            LOG.error("Couldn't find {}", args[0]);
        } catch (IOException e) {
            LOG.error("Problem reading {}", args[0]);
        }

        Graph graph = CSVToGraph.parse("src/main/resources/" +
                config.getProperty(Constants.CONFIG_SCENARIO_FILE));
        Random random = new Random(Long.parseLong(
                config.getProperty(Constants.CONFIG_RANDOM_SEED)));
        MessageGen messageGen = new MessageGen(graph, random, config);
        Gson gson;
        if (config.getProperty(Constants.CONFIG_PRETTY_JSON).equals("true")) {
            gson = new GsonBuilder().setPrettyPrinting()
                    .disableHtmlEscaping().create();
        } else {
            gson = new GsonBuilder().disableHtmlEscaping().create();
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getProperty(Constants.CONFIG_KAFKA_HOST) + ":9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        for (Collection<Message> coll: messageGen.getIterable()) {
            LOG.info("-----");
            for (Message m : coll) {
                LOG.info(new String(gson.toJson(m).getBytes("UTF-8")));
                producer.send(new ProducerRecord<>(
                        config.getProperty(Constants.CONFIG_MESSAGE_TOPIC),
                        Long.toString(m.getId()),
                        new String(gson.toJson(m).getBytes("UTF-8"))));
            }
            for (Query q : messageGen.getQueries()) {
                q.preSerialize();
                LOG.info(new String(gson.toJson(q).getBytes("UTF-8")));
                producer.send(new ProducerRecord<>(config.getProperty(
                        Constants.CONFIG_QUERY_TOPIC),
                        q.getQueryRoute(),
                        new String(gson.toJson(q).getBytes("UTF-8"))));
            }
        }
        producer.close();

    }
}
