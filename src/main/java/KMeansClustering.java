

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import org.apache.spark.api.java.function.Function;
import scala.Int;
import scala.Tuple2;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class KMeansClustering {
    private static final double CONVERGENCE_THRESHOLD = 0.0001;

    public static void main(String[] args) throws Exception {

        String inputFile = args[0];
        String outputFile = args[1];
        String groundTruthPath = args[2];
        int numberOfClusters = Integer.parseInt(args[3]);
        int maxIterations = Integer.parseInt(args[4]);

        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Distance calculator
        EuclideanCalculator distanceCalculator = new EuclideanCalculator();

        // Initialize start time
        final long startTime = System.nanoTime();

        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<DataTuple> datapoints = input.map(l -> new DataTuple(l, distanceCalculator));
        long dataSetSize = datapoints.count();
        System.out.println("Dataset has " + dataSetSize + " samples to cluster");

        // Picking initial centroids
        List<DataTuple>  initialSamples = datapoints.takeSample(false, numberOfClusters);
        List<CentroidDataTuple> centroids = new ArrayList<>();
        for (DataTuple dataPoint : initialSamples) {
            centroids.add(new CentroidDataTuple(dataPoint));
        }

        int iterationsCnt = 1;
        boolean converged = false;
        while (!converged && iterationsCnt <= maxIterations) {
            // Mapping each dataTuple to centroid
            JavaPairRDD <CentroidDataTuple, Tuple2<DataTuple,Integer>> mappedDataPoints = datapoints.mapToPair(new Mapper(centroids));
            // Summing all the values of the dataTuples and counting the number of tuples for each centroid
            JavaPairRDD<CentroidDataTuple, Tuple2<DataTuple,Integer>> summedCentroids = mappedDataPoints.reduceByKey(
                    new Function2<Tuple2<DataTuple,Integer>, Tuple2<DataTuple,Integer>, Tuple2<DataTuple, Integer>>() {
                        public Tuple2<DataTuple, Integer> call(Tuple2<DataTuple,Integer> x, Tuple2<DataTuple,Integer> y) {
                            x._1.add(y._1);
                            return new Tuple2<>(x._1, x._2 + y._2);
                        }
                    });

            // Calculating the new centroids by averaging the dataTuples for each centroid
            JavaRDD<CentroidDataTuple> newCentroids = summedCentroids.map(new Function<Tuple2<CentroidDataTuple, Tuple2<DataTuple, Integer>>,
                    CentroidDataTuple>() {
                @Override
                public CentroidDataTuple call(Tuple2<CentroidDataTuple, Tuple2<DataTuple, Integer>> summedDataTuple) {
                    summedDataTuple._2._1.divide(summedDataTuple._2._2);
                    return new CentroidDataTuple(summedDataTuple._2._1);
                }
            });

            // Check if algorithm has converged
            converged = isConverged(centroids, newCentroids.collect());

//            System.out.println("At iteration #" + iterationsCnt + ":");
//            newCentroids.foreach(data -> {
//                System.out.println("Centroid = " + data.toString());
//            });

            // Update Centroids
            centroids = newCentroids.collect();

            // If converged, write output to file, else update iterations counter
            if (converged) {
                newCentroids.saveAsTextFile(outputFile);
            } else {
                iterationsCnt++;
            }
        }
        System.out.println("Algorithm Converged after " + iterationsCnt + " iteration(s)");
        final long endTime = System.nanoTime();
        System.out.println("Run time: " + ((endTime - startTime) / 1000000) + "ms");

    }

    private static boolean isConverged(List<CentroidDataTuple> oldCentroids, List<CentroidDataTuple> newCentroids) {
        for (int i = 0 ; i < oldCentroids.size(); i++) {
            if (oldCentroids.get(i).getDifference(newCentroids.get(i)) > CONVERGENCE_THRESHOLD) {
                return false;
            }
        }
        return true;
    }
}
