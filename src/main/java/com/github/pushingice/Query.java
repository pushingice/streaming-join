package com.github.pushingice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query {


    private List<String> queryNodes = new ArrayList<>();
    private Map<String, String> treeContent = new HashMap<>();
    private String leafType = "";
    private Map<Long, String> leafContent = new HashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(
            Driver.class.getCanonicalName());

    public String getQueryRoute() {
        return String.join("/", queryNodes);
    }

    public void setQueryNodes(List<String> nodes) {
        queryNodes = nodes;
    }

    public List<String> getQueryNodes() {
        return queryNodes;
    }


    public void addQueryNode(String type, long id) {
        queryNodes.add(type);
        queryNodes.add(Long.toString(id));
    }

    public void addTreeContent(String type, String content) {
        treeContent.put(type, content);
    }

    public void addLeafContent(String leafType, long id, String content) {
        this.leafType = leafType;
        leafContent.put(id, content);
    }

    public boolean mergeIfPossible(Query other) {
        int len = other.getQueryNodes().size();

        if (queryNodes.isEmpty()) {
            for (int i = 0; i < len - 2; i++) {
                queryNodes.add(other.getQueryNodes().get(i));
            }
            leafType = other.getQueryNodes().get(len-2);
            queryNodes.add(leafType + "s");

        } else {
            for (int i = 0; i < len - 2; i++) {
                String otherNode = other.queryNodes.get(i);
                String thisNode = queryNodes.get(i);
                if (!otherNode.equals(thisNode)) {
                    return false;
                }
            }

        }
        treeContent = other.treeContent;
        long otherId = Long.parseLong(other.getQueryNodes().get(len - 1));
        leafContent.put(otherId, treeContent.get(leafType));

        return true;
    }

    @Override
    public String toString() {
        return "Query{" +
                "queryString=" + getQueryRoute() +
                ", treeContent=" + treeContent.keySet() +
                ", leafType='" + leafType + '\'' +
                ", leafContent=" + leafContent.keySet() +
                '}';
    }
}
