package com.github.pushingice;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class Driver {

    public static void main(String[] args) {
        System.out.println("hello");

        File csvData = new File("src/main/resources/minDirected.csv");
        try (CSVParser parser = CSVParser.parse(
                csvData, Charset.defaultCharset(), CSVFormat.DEFAULT)) {
            for (CSVRecord csvRecord : parser) {
                System.out.println(csvRecord.get(0));
                System.out.println(csvRecord.get(1));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
