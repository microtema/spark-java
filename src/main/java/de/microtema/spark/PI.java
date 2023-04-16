package de.microtema.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public final class PI {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("PI")
                .master("local")
                .getOrCreate();

        try (var jsc = new JavaSparkContext(spark.sparkContext())) {

            var slices = 2;
            var n = 100000 * slices;

            var list = new ArrayList<Integer>(n);
            for (int i = 0; i < n; i++) {
                list.add(i);
            }

            var dataSet = jsc.parallelize(list, slices);

            var count = dataSet.map(integer -> {
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y <= 1) ? 1 : 0;
            }).reduce(Integer::sum);

            System.out.println("Pi is roughly " + 4.0 * count / n);
        }
    }
}