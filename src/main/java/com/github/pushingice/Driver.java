package com.github.pushingice;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
        Random random = new Random();
        MessageGen messageGen = new MessageGen(graph, random, config);
        Gson gson;
        if (config.getProperty(Constants.CONFIG_PRETTY_JSON).equals("true")) {
            gson = new GsonBuilder().setPrettyPrinting()
                    .disableHtmlEscaping().create();
        } else {
            gson = new GsonBuilder().disableHtmlEscaping().create();
        }

        for (Collection<Message> coll: messageGen.getIterable()) {
            LOG.info("-----");
//            SortedSet<Message> sorted = new TreeSet<>(new MessageComparator());
//            sorted.addAll(coll);
            for (Message m : coll) {
                LOG.info(new String(gson.toJson(m).getBytes("UTF-8")));
            }
            for (Query q : messageGen.getQueries()) {
                q.preSerialize();
                LOG.info(new String(gson.toJson(q).getBytes("UTF-8")));
            }
        }



    }
}
