package com.github.pushingice;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class MessageGen {

    private int messageCount;
    private Graph graph;
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

            Map<String, Message> msgMap = new HashMap<>();
            graph.edges().forEachRemaining(e -> {
                String fromType = e.outVertex().label();
                String toType = e.inVertex().label();
                Message fromMsg = getOrCreate(msgMap, fromType);
                Message toMsg = getOrCreate(msgMap, toType);
                fromMsg.setFkId(toMsg.getId());
                fromMsg.setFkMessageType(toMsg.getMessageType());
            });
            count += msgMap.size();
            return msgMap.values();
        }

    }

    private class MessageIterable implements Iterable<Collection<Message>> {

        @Override
        public Iterator<Collection<Message>> iterator() {
            return new MessageIterator();
        }
    }

    public MessageGen(Graph graph, Random random, Properties config) {
        this.messageCount = Integer.parseInt(
                config.getProperty(Constants.CONFIG_MESSAGE_COUNT));
        this.graph = graph;
        this.random = random;
        this.config = config;
    }

    public Iterable<Collection<Message>> getIterable() {
        return new MessageIterable();
    }


}
