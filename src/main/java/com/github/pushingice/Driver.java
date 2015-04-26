package com.github.pushingice;

public class Driver {

    public static void main(String[] args) {
        System.out.println("hello");

        EdgeList graph = new EdgeList("src/main/resources/minDirected.csv");
        System.out.println(graph.toString());

    }
}
