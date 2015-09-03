package com.github.pushingice.impls.sqlite.web;
import com.github.pushingice.Message;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static spark.Spark.get;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(
            App.class.getSimpleName());

    private static String getJSONResults(
            String query, String... tables) {

        Set<Message> results = new HashSet<>();
        try (Connection connection = DriverManager
                .getConnection("jdbc:sqlite:message.db")) {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                int c = 1;
                for (String table : tables) {
                    results.add(new Message(
                            table, rs.getLong(c), "", 0L, rs.getString(c+1)));
                    c += 2;
                }
            }

        } catch (SQLException e) {
            LOG.info("{}", e);
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(results, Set.class);

    }

    public static void main(String[] args) {

        get("/", (req, res) -> "{\"hello\": \"world\"}");

        get("/As", "application/json", (req, res) -> {

            return getJSONResults("SELECT * FROM a;", "a");

        });

        get("/A/:a_id/Bs", (request, response) -> {

            String query = String.format("SELECT a.id, a.content, " +
                    "b.id, b.content FROM a " +
                    "JOIN a_b JOIN b " +
                    "WHERE a.id == %s AND " +
                    "b.id == a_b.b_id;", request.params(":a_id"));

            return getJSONResults(query, "a", "b");

        });

        get("/A/:a_id/B/:b_id/Ds", (request, response) -> {

            String query = String.format("SELECT a.id, a.content, " +
                            "b.id, b.content, " +
                            "d.id, d.content " +
                            "FROM a JOIN a_b " +
                            "JOIN b JOIN b_d " +
                            "JOIN d " +
                            "WHERE a.id == %s AND " +
                            "b.id == a_b.b_id AND " +
                            "b.id == %s AND " +
                            "d.id == b_d.d_id;",
                    request.params(":a_id"), request.params(":b_id"));

            return getJSONResults(query, "a", "b", "d");
        });



    }
}
