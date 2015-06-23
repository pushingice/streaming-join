package com.github.pushingice;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Driver {

    private static final Logger LOG = LoggerFactory.getLogger(
            Driver.class.getCanonicalName());

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please specify properties file.");
            System.exit(0);
        }
        Properties prop = new Properties();
        try (InputStream input = new FileInputStream(args[0])) {
            prop.load(input);
            LOG.info("{}:", args[0]);
            prop.entrySet().forEach((x) ->
                    LOG.info("{}={}", x.getKey(), x.getValue()));
        } catch (FileNotFoundException e) {
            LOG.error("Couldn't find {}", args[0]);
        } catch (IOException e) {
            LOG.error("Problem reading {}", args[0]);
        }
        Graph g = CSVToGraph.parse("src/main/resources/" +
                prop.getProperty(Constants.CONFIG_SCENARIO_FILE));

    }
}
