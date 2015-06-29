package com.github.pushingice;

import org.apache.tinkerpop.gremlin.structure.Graph;
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

        private Message getOrCreate(Map<String, Message> map, String key) {
            if (!map.containsKey(key)) {
                map.put(key, new Message(config, random, key));
            }
            return map.get(key);
        }

        @Override
        public Collection<Message> next() {

            List<Message> msgs = new ArrayList<>();
            Graph messageGraph = TinkerGraph.open();
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
                    LOG.info("no nodes for from type {}, adding", fromType);
                    IntStream.range(0, fromWeight).forEach(x ->
                            messageGraph.addVertex(fromType,
                                    new Message(config, random, fromType)));
                    messageGraph.traversal().V().has(fromType).forEachRemaining(
                            v -> LOG.info(v.property(fromType).value().toString())
                    );
                }

                if (!hasTo) {
                    LOG.info("no nodes for to type {}, adding", toType);
                    IntStream.range(0, toWeight).forEach(x ->
                            messageGraph.addVertex(toType,
                                    new Message(config, random, toType)));
                    messageGraph.traversal().V().has(toType).forEachRemaining(
                            v -> LOG.info(v.property(toType).value().toString())
                    );
                }

                messageGraph.traversal().V().has(toType).forEachRemaining(t ->
                    messageGraph.traversal().V().has(fromType).forEachRemaining(f -> {
                        Message from = ((Message) f.property(fromType).value()).copy();
                        Message to = ((Message) t.property(toType).value()).copy();
                        from.setFkId(to.getId());
                        from.setFkMessageType(to.getMessageType());
                        msgs.add(from);
                        msgs.add(to);
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
