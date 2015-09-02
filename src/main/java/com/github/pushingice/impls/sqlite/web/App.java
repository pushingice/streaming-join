package com.github.pushingice.impls.sqlite.web;
import com.github.pushingice.Message;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static spark.Spark.get;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(
            App.class.getSimpleName());

    public static void main(String[] args) {



        get("/hello", (req, res) -> "Hello World");

        get("/", (req, res) -> {
            res.type("text/json");
            return "{\"hello\": \"world\"}";
        });

        get("/As", (req, res) -> {
            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            List<Message> results = new ArrayList<>();
            try (Connection connection = DriverManager
                    .getConnection("jdbc:sqlite:message.db")) {
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT * FROM a;");
                while (rs.next()) {
                    LOG.info("{}", rs.getInt("id"));
                    Message msg = new Message("a", rs.getLong("id"), "", 0L, rs.getString("content"));
                    results.add(msg);
                }

            } catch (SQLException e) {
                LOG.info("{}", e);
            }
            return gson.toJson(results, List.class);
        });




    }
}
