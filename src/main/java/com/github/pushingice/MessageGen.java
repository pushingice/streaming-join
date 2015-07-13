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
            // collects messages to send
            List<Message> msgs = new LinkedList<>();
            // use Tinkerpop to build and query a message graph
            Graph contentGraph = TinkerGraph.open();
            // traverse the model graph in a tree-like fashion (query)
            GraphTraversal traversal = modelGraph.traversal().V().out();
            Tree tree = (Tree) traversal.tree().next();
            List<List<String>> queryPaths = treeversal(tree);
            // iterate over each query path (e.g. A->B->C, A->B->D, ...)
            queryPaths.forEach(query -> {
                // create two staggered iterators (e.g. A->B... and B->C...)
                Iterator<String> fromIterator = query.iterator();
                Iterator<String> toIterator = query.iterator();
                toIterator.next();
                while (toIterator.hasNext()) {
                    String fromType = fromIterator.next();
                    String toType = toIterator.next();
                    // grab weights from the model graph
                    List<Integer> weights = new ArrayList<>();
                    modelGraph.traversal().V().hasLabel(fromType)
                            .out().hasLabel(toType).inE()
                            .forEachRemaining(x -> {
                                Object w1, w2;
                                w1 = x.property(Constants.FROM_WEIGHT).value();
                                w2 = x.property(Constants.TO_WEIGHT).value();
                                weights.add(Integer.parseInt((String) w1));
                                weights.add(Integer.parseInt((String) w2));
                            });
                    int fromWeight = weights.get(0);
                    int toWeight = weights.get(1);
                    // accumulate outgoing and incoming messages
//                    List<Message> fromMsgs = new LinkedList<>();
//                    List<Message> toMsgs = new LinkedList<>();
                    SortedSet<Message> fromMsgs = new TreeSet<>(new MessageComparator());
                    SortedSet<Message> toMsgs = new TreeSet<>(new MessageComparator());
                    // reference to message vertex
                    Vertex fromV, toV;
                    // loop over all messages with the 'from' type (e.g. 'A')
                    GraphTraversal fromTrav = contentGraph.traversal().V()
                            .hasLabel(fromType);
                    // if there are no messages of that type
                    if (!fromTrav.hasNext()) {
                        // add fromWeight # of messages to the graph
                        for (int i = 0; i < fromWeight; i++) {
                            Message fromMsg = new Message(
                                    config, random, fromType);
                            fromV = contentGraph.addVertex(T.label, fromType,
                                    Constants.MSG_TYPE,
                                    fromMsg.getMessageType(),
                                    Constants.MSG_ID, fromMsg.getId(),
                                    Constants.MSG, fromMsg);
                            msgs.add(fromMsg);
                            fromMsgs.add(fromMsg);
                        }
                    } else {
                        // otherwise iterate over existing nodes of that type
                        while (fromTrav.hasNext()) {
                            // grab a reference to the existing vertex
                            fromV = (Vertex) fromTrav.next();
                            Message fromMsg = (Message) fromV
                                    .property(Constants.MSG).value();
                            fromMsgs.add(fromMsg);
                        }
                    }
                    // loop over all messages with the 'to' type (e.g. 'B')
                    GraphTraversal toTrav = contentGraph.traversal().V()
                            .hasLabel(toType);
                    // if there are no messages of that type
                    if (!toTrav.hasNext()) {
                        // create toWeight # of messages for each parent
                        // message
                        for (int i = 0; i < fromMsgs.size()*toWeight; i++) {
                            Message toMsg = new Message(config, random, toType);
                            toV = contentGraph.addVertex(T.label, toType,
                                    Constants.MSG_ID, toMsg.getId(),
                                    Constants.MSG_TYPE, toMsg.getMessageType(),
                                    Constants.MSG, toMsg);
                            msgs.add(toMsg);
                            toMsgs.add(toMsg);
                        }
                    } else {
                        // otherwise grab a reference to existing vertex
                        while (toTrav.hasNext()) {
                            toV = (Vertex) toTrav.next();
                            Message toMsg = (Message) toV
                                    .property(Constants.MSG).value();
                            toMsgs.add(toMsg);
                        }

                    }

                    long minFromId = fromMsgs.first().getId();
                    // iterate over messages of 'from' type
                    for (Message fromMsg : fromMsgs) {
                        // get the vertex with that message id from the graph
                        GraphTraversal fromT = contentGraph.traversal().V()
                                .has(Constants.MSG_ID, fromMsg.getId());
                        fromV = (Vertex) fromT.next();
                        // iterate over messages of 'to' type
                        long minToId = toMsgs.first().getId();
                        for (Message toMsg : toMsgs) {
                            // get the vertex with that id from the graph
                            GraphTraversal toT = contentGraph.traversal().V()
                                    .has(Constants.MSG_ID, toMsg.getId());
                            toV = (Vertex) toT.next();
                            // find the edge connecting 'from' and 'to'
                            GraphTraversal edgeT = contentGraph.traversal().E()
                                    .has(Constants.MSG_ID, fromMsg.getId())
                                    .has(Constants.TO_MSG_ID, toMsg.getId());
                            // if it doesn't exist, add it
                            // this math relies on sequential ids
                            // it paritions the messages to their parents
                            if (!edgeT.hasNext() &&
                                    (toMsg.getId() - minToId)/toWeight ==
                                            fromMsg.getId() - minFromId) {
                                Message linkMsg = fromMsg.copy();
                                linkMsg.setFkId(toMsg.getId());
                                linkMsg.setFkMessageType(
                                        toMsg.getMessageType());
                                linkMsg.setContent("");
                                fromV.addEdge(Constants.FOREIGN_KEY, toV,
                                        Constants.MSG, linkMsg,
                                        Constants.MSG_TYPE,
                                        fromMsg.getMessageType(),
                                        Constants.TO_MSG_TYPE,
                                        toMsg.getMessageType(),
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
                        Message m = (Message)
                                ((Vertex) v).property(Constants.MSG).value();
                        query.addQueryNode(m.getMessageType(), m.getId());
                        query.addTreeContent(m.getMessageType(),
                                m.getContent());
                    }
//                    LOG.info("q {}", query);
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
