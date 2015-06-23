package com.github.pushingice;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.*;

public class MessageGen {

    private int messageCount;
    private Graph graph;
    private Random random;
    private Properties config;
    private class MessageIterator implements Iterator<Message> {

        private int count = 0;

        @Override
        public boolean hasNext() {
            if (count < messageCount) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Message next() {
            count++;
            return new Message(config, random);
        }
    }
    private class MessageIterable implements Iterable<Message> {

        @Override
        public Iterator<Message> iterator() {

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

    public Iterable<Message> getIterable() {
        return new MessageIterable();
    }


}
