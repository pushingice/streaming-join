package com.github.pushingice.impls.sqlite.web;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

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
            try (Connection connection = DriverManager
                    .getConnection("jdbc:sqlite:message.db")) {
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT * FROM a;");
                while (rs.next()) {
                    LOG.info("{}", rs.getInt("id"));
                }

            } catch (SQLException e) {
                LOG.info("{}", e);
            }
            return "wat";
        });




    }
}
