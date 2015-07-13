package com.github.pushingice;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
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
    private int queryDepth;


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
            List<Message> msgs = new LinkedList<>();
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
                    List<Message> fromMsgs = new LinkedList<>();
                    List<Message> toMsgs = new LinkedList<>();
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
                        GraphTraversal fromT = contentGraph.traversal().V()
                                .has(Constants.MSG_ID, fromMsg.getId());
                        if (fromT.hasNext()) {
                            fromV = (Vertex) fromT.next();
                        } else {
                            fromV = contentGraph.addVertex(T.label, fromType,
                                    Constants.MSG_ID, fromMsg.getId(),
                                    Constants.MSG_TYPE, fromMsg.getMessageType(),
                                    Constants.MSG, fromMsg);
                        }

                        for (Message toMsg : toMsgs) {
                            GraphTraversal toT = contentGraph.traversal().V()
                                    .has(Constants.MSG_ID, toMsg.getId());
                            if (toT.hasNext()) {
                                toV = (Vertex) toT.next();
                            } else {
                                toV = contentGraph.addVertex(T.label, toType,
                                    Constants.MSG_ID, toMsg.getId(),
                                    Constants.MSG_TYPE, toMsg.getMessageType(),
                                    Constants.MSG, toMsg);
                            }

                            GraphTraversal edgeT = contentGraph.traversal().E()
                                    .has(Constants.MSG_ID, fromMsg.getId())
                                    .has(Constants.TO_MSG_ID, toMsg.getId());
                            if (!edgeT.hasNext()) {
                                Message linkMsg = fromMsg.copy();
                                linkMsg.setFkId(toMsg.getId());
                                linkMsg.setFkMessageType(toMsg.getMessageType());
                                linkMsg.setContent("");
                                fromV.addEdge(Constants.FOREIGN_KEY, toV,
                                        Constants.MSG, linkMsg,
                                        Constants.MSG_TYPE, fromMsg.getMessageType(),
                                        Constants.TO_MSG_TYPE, toMsg.getMessageType(),
                                        Constants.MSG_ID, fromMsg.getId(),
                                        Constants.TO_MSG_ID, toMsg.getId());

                                msgs.add(linkMsg);
                            }


                        }
                    }
                }

            });

            for(List<String> path : queryPaths) {

                LOG.info("p {}", path);
                if (path.size() > queryDepth) {
                    continue;
                }
                GraphTraversal<Vertex, Path> contentT = null;
                switch (path.size()) {
                    case 2:
                        contentT = contentGraph.traversal().V()
                                .has(Constants.MSG_TYPE, path.get(0)).out()
                                .has(Constants.MSG_TYPE, path.get(1)).path();
                        break;
                    case 3:
                        contentT = contentGraph.traversal().V()
                                .has(Constants.MSG_TYPE, path.get(0)).out()
                                .has(Constants.MSG_TYPE, path.get(1)).out()
                                .has(Constants.MSG_TYPE, path.get(2)).path();
                        break;
                    case 4:
                        contentT = contentGraph.traversal().V()
                                .has(Constants.MSG_TYPE, path.get(0)).out()
                                .has(Constants.MSG_TYPE, path.get(1)).out()
                                .has(Constants.MSG_TYPE, path.get(2)).out()
                                .has(Constants.MSG_TYPE, path.get(3)).path();
                        break;
                    default:
                        LOG.info("Depth {} not supported yet", path.size());
                }

                List<Query> queries = new ArrayList<>();
                while(contentT != null && contentT.hasNext()) {
                    List<Object> itemO = contentT.next().objects();
                    Query query = new Query();
                    for (Object v : itemO) {
                        Message m = (Message) ((Vertex) v).property(Constants.MSG).value();
                        query.addQueryNode(m.getMessageType(), m.getId());
                        query.addTreeContent(m.getMessageType(), m.getContent());
                    }
                    LOG.info("q {}", query);
                    queries.add(query);
                }

                boolean nextMerge = true;
                Query merged = new Query();
                List<Query> mergedQueries = new ArrayList<>();
                for (Query q : queries) {
                    if (nextMerge) {
                        merged = new Query();
                        nextMerge = !merged.mergeIfPossible(q);
                    } else {
                        nextMerge = !merged.mergeIfPossible(q);
                        if (nextMerge) {
                            mergedQueries.add(merged);
                            merged = new Query();
                            nextMerge = !merged.mergeIfPossible(q);
                        }
                    }
                }
                mergedQueries.add(merged);
                mergedQueries.forEach(x -> LOG.info("m {}", x));

            }

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
        this.queryDepth = Integer.parseInt(
                config.getProperty(Constants.CONFIG_QUERY_DEPTH));
    }

    public Iterable<Collection<Message>> getIterable() {
        return new MessageIterable();
    }


}
