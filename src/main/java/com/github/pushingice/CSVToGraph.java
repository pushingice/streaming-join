package com.github.pushingice;

import com.github.pushingice.Constants.Kind;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVToGraph {

    private static final Logger LOG = LoggerFactory.getLogger(
            CSVToGraph.class);

    private static void addEdge(CSVRecord rec, Graph g) {
        Vertex v0 = g.addVertex(rec.get(0));
        Vertex v1 = g.addVertex(rec.get(1));
        v0.addEdge(Kind.FOREIGN_KEY.toString(), v1, "weight", rec.get(2));


    }

    public static void LogAllEdges(Graph g) {
        g.traversal().E().forEachRemaining(x -> LOG.info("{} -[{}]-> {} : {}",
                x.outVertex().label(), x.label(), x.inVertex().label(),
                x.property("weight")));
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
