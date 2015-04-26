package com.github.pushingice;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EdgeList {

    private Map<String, String> edgeList;
    private final Logger LOG = LoggerFactory.getLogger(
            this.getClass().getCanonicalName());

    public EdgeList (String filename) {

        try (CSVParser parser = CSVParser.parse(new File(filename),
                Charset.defaultCharset(), CSVFormat.DEFAULT)) {
            for (CSVRecord csvRecord : parser) {
                LOG.info(csvRecord.get(0));
                LOG.info(csvRecord.get(1));
            }
        } catch (IOException e) {
            LOG.error("Unable to open {}", filename);
        }

    }
}
