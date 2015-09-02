package com.github.pushingice.impls.sqlite.web;
import com.github.pushingice.Message;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import static spark.Spark.get;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(
            App.class.getSimpleName());

    public static void main(String[] args) {



        get("/hello", (req, res) -> "Hello World");

        get("/", (req, res) -> {
            res.type("application/json");
            return "{\"hello\": \"world\"}";
        });

        get("/As", "application/json", (req, res) -> {
            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            Set<Message> results = new HashSet<>();
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
            return gson.toJson(results, Set.class);
        });

        get("/A/:a_id/Bs", (req, res) -> {
            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            Set<Message> results = new HashSet<>();
            try (Connection connection = DriverManager
                    .getConnection("jdbc:sqlite:message.db")) {
                Statement statement = connection.createStatement();
                String stmt = String.format("SELECT a.id, a.content FROM a " +
                        "JOIN a_b JOIN b " +
                        "WHERE a.id == %s AND " +
                        "b.id == a_b.b_id;", req.params(":a_id"));
                LOG.info("{}", stmt);
                ResultSet rs = statement.executeQuery(stmt);
                while (rs.next()) {
                    LOG.info("{}", rs.getInt(1));
                    Message msg = new Message("a", rs.getLong(1), "", 0L, rs.getString(2));
                    results.add(msg);
                }
            } catch (SQLException e) {
                LOG.info("{}", e);
            }
            return gson.toJson(results, Set.class);
        });

    }
}
