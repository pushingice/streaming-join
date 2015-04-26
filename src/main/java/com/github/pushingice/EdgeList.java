package com.github.pushingice;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EdgeList {

    private Map<String, String> edgeList = new HashMap<>();
    private final Logger LOG = LoggerFactory.getLogger(
            this.getClass().getCanonicalName());

    public EdgeList (String filename) {

        try (CSVParser parser = CSVParser.parse(new File(filename),
                Charset.defaultCharset(), CSVFormat.DEFAULT)) {
            parser.getRecords().forEach(
                    (x) -> edgeList.put(x.get(0), x.get(1)));
        } catch (IOException e) {
            LOG.error("Unable to open {}", filename);
        }

    }
}
