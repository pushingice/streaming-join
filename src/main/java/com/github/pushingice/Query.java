package com.github.pushingice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query {


    private List<String> queryNodes = new ArrayList<>();
    private List<String> leafNodes = new ArrayList<>();
    private Map<String, String> treeContent = new HashMap<>();
    private String leafType = "";
    private Map<Long, String> leafContent = new HashMap<>();

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

    public void merge(Query other) {
        int len = other.getQueryNodes().size();
        for (int i = 0; i < len-2; i++) {
            queryNodes.add(other.getQueryNodes().get(i));
        }
        leafType = other.getQueryNodes().get(len-2);
        queryNodes.add(leafType + "s");
        leafNodes.add(other.getQueryNodes().get(len - 1));

    }

    @Override
    public String toString() {
        return "Query{" +
                "queryString=" + getQueryRoute() +
                ", treeContent=" + treeContent.keySet() +
                ", leafType='" + leafType + '\'' +
                ", leafNodes=" + leafNodes +
                ", leafContent=" + leafContent +
                '}';
    }
}
