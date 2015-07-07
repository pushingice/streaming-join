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

        Graph contentGraph = TinkerGraph.open();
        Graph baseGraph = TinkerGraph.open();
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
                        Message link = from.copy();
                        link.setFkId(to.getId());
                        link.setFkMessageType(to.getMessageType());
                        link.setContent("");
                        fromV.addEdge(Constants.FOREIGN_KEY, toV,
                                Constants.MSG, link);
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
            Set<Message> msgs = new HashSet<>();
            Graph contentGraph = TinkerGraph.open();
            GraphTraversal traversal = modelGraph.traversal().V().out();

            Tree tree = (Tree) traversal.tree().next();
            List<List<String>> queries = treeversal(tree);
            queries.forEach(q -> {
                LOG.info("q {}", q);
                Iterator<String> fromIterator = q.iterator();
                Iterator<String> toIterator = q.iterator();
                toIterator.next();
                while(toIterator.hasNext()) {
                    String fromType = fromIterator.next();
                    String toType = toIterator.next();
                    LOG.info("from {}", fromType);
                    LOG.info("to {}", toType);
                    List<Integer> weights = new ArrayList<>();
                    modelGraph.traversal().V().hasLabel(fromType)
                            .out().hasLabel(toType).inE()
                            .forEachRemaining(x -> {
                                weights.add(Integer.parseInt((String) x.property(Constants.FROM_WEIGHT).value()));
                                weights.add(Integer.parseInt((String) x.property(Constants.TO_WEIGHT).value()));
                            });
                    int fromWeight = weights.get(0);
                    int toWeight = weights.get(1);
                    Set<Message> fromMsgs = new HashSet<>();
                    Set<Message> toMsgs = new HashSet<>();
                    Vertex fromV, toV;

                    GraphTraversal fromTrav = contentGraph.traversal().V()
                            .hasLabel(fromType);
                    if (!fromTrav.hasNext()) {
                        for (int i = 0; i < fromWeight; i++) {
                            Message fromMsg = new Message(config, random, fromType);
                            fromV = contentGraph.addVertex(T.label, fromType,
                                    Constants.MSG_ID, fromMsg.getId(),
                                    Constants.MSG, fromMsg);
                            msgs.add(fromMsg);
                            fromMsgs.add(fromMsg);
                        }
                    } else {
                        while (fromTrav.hasNext()) {
                            fromV = (Vertex) fromTrav.next();
                            Message fromMsg = (Message) fromV
                                    .property(Constants.MSG).value();
                            fromMsgs.add(fromMsg);
                        }
                    }

                    GraphTraversal toTrav = contentGraph.traversal().V()
                            .hasLabel(toType);

                    if (!toTrav.hasNext()) {
                        for (int i = 0; i < toWeight; i++) {
                            Message toMsg = new Message(config, random, toType);
                            msgs.add(toMsg);
                            toMsgs.add(toMsg);
                        }
                    } else {
                        while (toTrav.hasNext()) {
                            toV = (Vertex) toTrav.next();
                            Message toMsg = (Message) toV
                                    .property(Constants.MSG).value();
                            toMsgs.add(toMsg);
                        }

                    }
                    for (Message fromMsg : fromMsgs) {
                        for (Message toMsg : toMsgs) {
                            toV = contentGraph.addVertex(T.label, toType,
                                    Constants.MSG_ID, toMsg.getId(),
                                    Constants.MSG, toMsg);
                            fromV = contentGraph.addVertex(T.label, fromType,
                                    Constants.MSG_ID, fromMsg.getId(),
                                    Constants.MSG, fromMsg);
                            Message link = fromMsg.copy();
                            link.setFkId(toMsg.getId());
                            link.setFkMessageType(toMsg.getMessageType());
                            link.setContent("");
                            fromV.addEdge(Constants.FOREIGN_KEY, toV,
                                    Constants.MSG, link);
                            msgs.add(link);
                        }
                    }
                }

            });
            count += msgs.size();
            return msgs;
        }

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
