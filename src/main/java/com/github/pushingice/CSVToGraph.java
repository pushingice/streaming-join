package com.github.pushingice;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVToGraph {

    private static final Logger LOG = LoggerFactory.getLogger(
            CSVToGraph.class);

    private static void addEdge(CSVRecord rec, Graph g) {
        String fromId = rec.get(0);
        String toId = rec.get(1);
        Vertex fromV, toV;
        if (g.traversal().V().hasLabel(fromId).hasNext()) {
            LOG.info("has node {}", fromId);
            fromV = g.traversal().V().hasLabel(fromId).next();
        } else {
            LOG.info("adding node {}", fromId);
            fromV = g.addVertex(fromId);
        }
        if (g.traversal().V().hasLabel(toId).hasNext()) {
            LOG.info("has node {}", toId);
            toV = g.traversal().V().hasLabel(toId).next();
        } else {
            LOG.info("adding node {}", toId);
            toV = g.addVertex(toId);
        }


        fromV.addEdge(Constants.FOREIGN_KEY, toV,
                Constants.FROM_WEIGHT, rec.get(2),
                Constants.TO_WEIGHT, rec.get(3));


    }

    public static void LogAllEdges(Graph g) {
        g.traversal().E().forEachRemaining(x -> LOG.info("{} -[{}]-> {} | {}:{}",
                x.outVertex().label(), x.label(), x.inVertex().label(),
                x.property(Constants.FROM_WEIGHT),
                x.property(Constants.TO_WEIGHT)));
    }

    public static Graph parse(String filename) {

        Graph g = TinkerGraph.open();

        try (CSVParser parser = CSVParser.parse(new File(filename),
                Charset.defaultCharset(), CSVFormat.DEFAULT)) {
            parser.getRecords().forEach(
                    x -> addEdge(x, g));
        } catch (IOException e) {
            LOG.error("Unable to open {}", filename);
        }

        LogAllEdges(g);
        return g;
    }
}
