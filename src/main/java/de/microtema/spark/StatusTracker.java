package de.microtema.spark;

import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public final class StatusTracker {

    public static final String APP_NAME = "StatusTracker";

    public static final class IdentityWithDelay<T> implements Function<T, T> {
        @Override
        public T call(T x) throws Exception {
            Thread.sleep(2 * 1000);  // 2 seconds
            return x;
        }
    }

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName(APP_NAME)
                .master("local")
                .getOrCreate();

        try (var jsc = new JavaSparkContext(spark.sparkContext())) {

            // Example of implementing a progress reporter for a simple job.
            var rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5).map(new IdentityWithDelay<>());
            var jobFuture = rdd.collectAsync();

            while (!jobFuture.isDone()) {
                Thread.sleep(1000);  // 1 second
                List<Integer> jobIds = jobFuture.jobIds();
                if (jobIds.isEmpty()) {
                    continue;
                }
                int currentJobId = jobIds.get(jobIds.size() - 1);
                JavaSparkStatusTracker javaSparkStatusTracker = jsc.statusTracker();
                SparkJobInfo jobInfo = javaSparkStatusTracker.getJobInfo(currentJobId);
                SparkStageInfo stageInfo = javaSparkStatusTracker.getStageInfo(jobInfo.stageIds()[0]);
                System.out.println(stageInfo.numTasks() + " tasks total: " + stageInfo.numActiveTasks() + " active, " + stageInfo.numCompletedTasks() + " complete");
            }

            System.out.println("Job results are: " + jobFuture.get());
        }
    }
}