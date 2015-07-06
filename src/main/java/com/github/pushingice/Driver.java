package com.github.pushingice;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Driver {

    private static final Logger LOG = LoggerFactory.getLogger(
            Driver.class.getCanonicalName());

    public static void main(String[] args) {

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
        Random random = new Random();
        MessageGen messageGen = new MessageGen(graph, random, config);

        for (Collection<Message> coll: messageGen.getIterable()) {
            LOG.info("-----");
//            for (Message m : coll) {
//                LOG.info(m.toString());
//            }
        }

    }
}
