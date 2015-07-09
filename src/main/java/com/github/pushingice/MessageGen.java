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


public class MessageGen {

    private int messageCount;
    private Graph modelGraph;
    private Random random;
    private Properties config;
    Set<List<String>> queries;


    private static final Logger LOG = LoggerFactory.getLogger(
            Driver.class.getCanonicalName());

    private void accumulate(Tree tree, List<List<String>> queries) {
        Vertex parent = (Vertex) tree.getObjectsAtDepth(1).get(0);
        List<Vertex> children = (List<Vertex>) tree.getObjectsAtDepth(2);

        children.forEach(child -> {

            List<String> query = new ArrayList<>();
            List<List<String>> extra = new ArrayList<>();
            query.add(parent.label());
            query.add(child.label());
            queries.add(query);

            queries.forEach(q -> {
                if (q.get(q.size() - 1).equals(parent.label()) &&
                        !q.contains(child.label())) {
                    List<String> newQuery = new ArrayList<>();
                    newQuery.addAll(q);
                    newQuery.add(child.label());
                    extra.add(newQuery);
                }
            });
            queries.addAll(extra);
        });
    }

    private List<List<String>> treeversal(Tree tree) {

        List<List<String>> queries = new ArrayList<>();
        if (!tree.isEmpty()) {
            List<Tree> lt = tree.splitParents();
            // Is there more than one subtree? If so, iterate over them
            if (lt.size() > 1) {
                lt.forEach(t -> accumulate(t, queries));
            }
        }
        return queries;
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
            List<List<String>> queryPaths = treeversal(tree);
            queryPaths.forEach(q -> {
                Iterator<String> fromIterator = q.iterator();
                Iterator<String> toIterator = q.iterator();
                toIterator.next();
                while (toIterator.hasNext()) {
                    String fromType = fromIterator.next();
                    String toType = toIterator.next();
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
                                    Constants.MSG_TYPE, fromMsg.getMessageType(),
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
                                    Constants.MSG_TYPE, toMsg.getMessageType(),
                                    Constants.MSG, toMsg);
                            fromV = contentGraph.addVertex(T.label, fromType,
                                    Constants.MSG_ID, fromMsg.getId(),
                                    Constants.MSG_TYPE, fromMsg.getMessageType(),
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

            queryPaths.forEach(path -> {
                LOG.info("p {}", path);
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
        this.queries = new HashSet<>();
    }

    public Iterable<Collection<Message>> getIterable() {
        return new MessageIterable();
    }


}
