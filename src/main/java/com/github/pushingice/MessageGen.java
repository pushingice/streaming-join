package com.github.pushingice;

import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.IntStream;


public class MessageGen {

    private int messageCount;
    private Graph modelGraph;
    private Random random;
    private Properties config;

    private static final Logger LOG = LoggerFactory.getLogger(
            Driver.class.getCanonicalName());

    private void accumulate(Tree tree, List<List<String>> queries) {
        Vertex parent = (Vertex) tree.getObjectsAtDepth(1).get(0);
        List<Vertex> children = (List<Vertex>) tree.getObjectsAtDepth(2);
        children.forEach(child -> {
            List<String> query = new ArrayList<>();
            query.add(parent.label());
            query.add(child.label());
            queries.add(query);
            queries.forEach(q -> {
                if (q.get(q.size()-1).equals(parent.label()) &&
                        !q.contains(child.label())) {
                    q.add(child.label());
                }
            });
        });
    }

    private List<List<String>> treeversal(Tree tree) {

        List<List<String>> queries = new ArrayList<>();
        if(!tree.isEmpty()) {
            List<Tree> lt = tree.splitParents();
            // Is there more than one subtree? If so, iterate over them
            if (lt.size() > 1) {
                lt.forEach(t -> accumulate(t, queries));
            }
        }
        return queries;
    }

    private Graph modelToMessageGraph() {
        Graph baseGraph = TinkerGraph.open();
        Graph contentGraph = TinkerGraph.open();
        modelGraph.edges().forEachRemaining(e -> {
            String fromType = e.outVertex().label();
            String toType = e.inVertex().label();
            int fromWeight = Integer.parseInt(
                    e.property(Constants.FROM_WEIGHT).value().toString());
            int toWeight = Integer.parseInt(
                    e.property(Constants.TO_WEIGHT).value().toString());
            boolean hasFrom = baseGraph.traversal().V()
                    .has(fromType).hasNext();
            boolean hasTo = baseGraph.traversal().V()
                    .has(toType).hasNext();
            if (!hasFrom) {
                IntStream.range(0, fromWeight).forEach(x ->
                        baseGraph.addVertex(fromType,
                                new Message(config, random, fromType)));
            }

            if (!hasTo) {
                IntStream.range(0, toWeight).forEach(x ->
                        baseGraph.addVertex(toType,
                                new Message(config, random, toType)));
            }

            // this will send dupes, consider it a feature
            GraphTraversal<Vertex, Vertex> baseTo = baseGraph.traversal()
                    .V().has(toType);
            GraphTraversal<Vertex, Vertex> baseFrom = baseGraph.traversal()
                    .V().has(fromType);
            baseTo.forEachRemaining(t -> baseFrom.forEachRemaining(f -> {
                        // Consider 'From' => 'To'
                        Message from = ((Message) f.property(fromType)
                                .value()).copy();
                        Message to = ((Message) t.property(toType)
                                .value()).copy();
                        // Send 'From'
                        Vertex fromV, toV;
                        GraphTraversal fromT = contentGraph.traversal()
                                .V().has(Constants.MSG_ID, from.getId());
                        if (!fromT.hasNext()) {
                            fromV = contentGraph.addVertex(T.label, fromType,
                                    Constants.MSG_ID, from.getId(),
                                    Constants.MSG, from);
                        } else {
                            fromV = (Vertex) fromT.next();
                        }
                        // Send 'To'
                        GraphTraversal toT = contentGraph.traversal()
                                .V().has(Constants.MSG_ID, to.getId());
                        if (!toT.hasNext()) {
                            toV = contentGraph.addVertex(T.label, toType,
                                    Constants.MSG_ID, to.getId(),
                                    Constants.MSG, to);
                        } else {
                            toV = (Vertex) toT.next();
                        }
                        // Send 'Edge Link'
                        from.setFkId(to.getId());
                        from.setFkMessageType(to.getMessageType());
                        from.setContent("");
                        fromV.addEdge(Constants.FOREIGN_KEY, toV,
                                Constants.MSG, from);
                    })
            );
        });
        return contentGraph;
    }


    private class MessageIterator implements Iterator<Collection<Message>> {

        private int count = 0;

        @Override
        public boolean hasNext() {
            return count < messageCount;
        }

        @Override
        public Collection<Message> next() {
            List<Message> msgs = new ArrayList<>();
            Graph contentGraph = modelToMessageGraph();
            GraphTraversal traversal = modelGraph.traversal().V().out();

            Tree tree = (Tree) traversal.tree().next();
            List<List<String>> queries = treeversal(tree);
            queries.forEach(q -> {
                LOG.info("q {}", q);
                Iterator<String> fromIterator = q.iterator();
                Iterator<String> toIterator = q.iterator();
                toIterator.next();
                while(toIterator.hasNext()) {
                    String from = fromIterator.next();
                    String to = toIterator.next();
                    LOG.info("from {}", from);
                    LOG.info("to {}", to);
                    // bwuh
                    contentGraph.traversal().V().hasLabel(from).out().hasLabel(to).inE()
                            .forEachRemaining(x -> LOG.info("e {}", x));
                }

            });
            contentGraph.traversal().E().forEachRemaining(x -> {
                msgs.add((Message) x.outVertex()
                        .property(Constants.MSG).value());
                msgs.add((Message) x.inVertex()
                        .property(Constants.MSG).value());
                msgs.add((Message) x.property(Constants.MSG).value());
            });
            Collections.shuffle(msgs);
            count += msgs.size();
            return msgs;
        }

//        private Graph messagesFromGraph() {
//            List<Message> msgs = new ArrayList<>();
//            Graph contentGraph = modelToMessageGraph();
//            Double deletePct = Double.parseDouble(
//                    config.getProperty(Constants.DELETE_PERCENT));
//
//            return contentGraph;
//
////                                    if (random.nextDouble() < deletePct) {
////                                        Message del = from.copy();
////                                        del.setCrudType(Constants.DELETE);
////                                        msgs.add(del);
////                                    }
//        }

    }

    private class MessageIterable implements Iterable<Collection<Message>> {

        @Override
        public Iterator<Collection<Message>> iterator() {
            return new MessageIterator();
        }
    }

    public MessageGen(Graph modelGraph, Random random, Properties config) {
        this.messageCount = Integer.parseInt(
                config.getProperty(Constants.CONFIG_MESSAGE_COUNT));
        this.modelGraph = modelGraph;
        this.random = random;
        this.config = config;
    }

    public Iterable<Collection<Message>> getIterable() {
        return new MessageIterable();
    }


}
