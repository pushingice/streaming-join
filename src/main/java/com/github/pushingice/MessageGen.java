package com.github.pushingice;

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


    private class MessageIterator implements Iterator<Collection<Message>> {

        private int count = 0;

        @Override
        public boolean hasNext() {
            return count < messageCount;
        }

        @Override
        public Collection<Message> next() {
            List<Message> msgs = messagesFromGraph();
            List<GraphTraversal> traversals = new ArrayList<>();
            traversals.add(modelGraph.traversal().V().out());
            traversals.add(modelGraph.traversal().V().out().out());
            traversals.add(modelGraph.traversal().V().out().out().out());
            traversals.add(modelGraph.traversal().V().out().out().out().out());


            traversals.forEach(trav -> {
                Tree tree = (Tree) trav.tree().next();
                if (!tree.isEmpty()) {
                    LOG.info("{} {}", tree, tree.isLeaf());
                    LOG.info("{}", ((Vertex) tree.keySet().iterator().next()).label());
                }
            });

            Collections.shuffle(msgs);
            return msgs;
        }

        private List<Message> messagesFromGraph() {
            List<Message> msgs = new ArrayList<>();
            Graph messageGraph = TinkerGraph.open();
            Double deletePct = Double.parseDouble(
                    config.getProperty(Constants.DELETE_PERCENT));

            modelGraph.edges().forEachRemaining(e -> {
                String fromType = e.outVertex().label();
                String toType = e.inVertex().label();
                int fromWeight = Integer.parseInt(
                        e.property(Constants.FROM_WEIGHT).value().toString());
                int toWeight = Integer.parseInt(
                        e.property(Constants.TO_WEIGHT).value().toString());
                boolean hasFrom = messageGraph.traversal().V()
                        .has(fromType).hasNext();
                boolean hasTo = messageGraph.traversal().V()
                        .has(toType).hasNext();
                if (!hasFrom) {
                    IntStream.range(0, fromWeight).forEach(x ->
                            messageGraph.addVertex(fromType,
                                    new Message(config, random, fromType)));
                }

                if (!hasTo) {
                    IntStream.range(0, toWeight).forEach(x ->
                            messageGraph.addVertex(toType,
                                    new Message(config, random, toType)));
                }

                messageGraph.traversal().V().has(toType).forEachRemaining(
                        t -> messageGraph.traversal().V().has(fromType)
                                .forEachRemaining(f -> {

                                    Message from = ((Message) f.property(fromType)
                                            .value()).copy();
                                    Message to = ((Message) t.property(toType)
                                            .value()).copy();
                                    from.setFkId(to.getId());
                                    from.setFkMessageType(to.getMessageType());
                                    from.setContent(to.getContent());
                                    msgs.add(from);
//                                    msgs.add(to);
                                    if (random.nextDouble() < deletePct) {
                                        Message del = from.copy();
                                        del.setCrudType(Constants.DELETE);
                                        msgs.add(del);
                                    }
                                })
                );
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
