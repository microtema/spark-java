package de.microtema.spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

public class Application {

    private static final String COMMA_DELIMITER = ",";

    public static void main(String[] args) {

        // Create Spark Session
        var spark = SparkSession
                .builder()
                .appName("microtema")
                .master("local")
                .getOrCreate();

        // Create SPark Context
        try (var jsc = new JavaSparkContext(spark.sparkContext())) {

            // Load Data
            var entries = jsc.textFile("src/main/resources/videos.csv");

            // Transforming data
            var titles = entries
                    .map(Application::extractTitle)
                    .filter(StringUtils::isNotBlank);

            var words = titles
                    .flatMap(Application::splitWords)
                    .filter(StringUtils::isNotBlank);

            // Counting data
            var wordCounts = words.countByValue();
            var sorted = wordCounts.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());

            // Display data
            sorted.forEach(it -> System.out.println(it.getKey() + ": " + it.getValue()));
        }
    }

    public static Iterator<String> splitWords(String it) {

        return Arrays.asList(it
                .toLowerCase()
                .trim()
                .replaceAll("\\p{Punct}", "")
                .split(" ")).iterator();
    }

    public static String extractTitle(String videoLine) {

        try {
            return videoLine.split(COMMA_DELIMITER)[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
}
