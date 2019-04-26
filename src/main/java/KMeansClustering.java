

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
    public static void main(String[] args) throws Exception {

        String inputFile = args[0];
        String outputFile = args[1];
        int numberOfClusters = Integer.parseInt(args[2]);

        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Distance calculator
        EuclideanCalculator distanceCalculator = new EuclideanCalculator();
        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<DataTuple> datapoints = input.map(l -> new DataTuple(l, distanceCalculator));
        double dataSetSize = (double) datapoints.count();

        //picking initial centroids
        List<DataTuple>  initialSamples = datapoints.takeSample(false, numberOfClusters);
        System.out.println(initialSamples.size());
        List<CentroidDataTuple> centroids = new ArrayList<>();
        for (DataTuple dataPoint : initialSamples) {
            centroids.add(new CentroidDataTuple(dataPoint));
        }



       //    Map each datapoint to a (datapoint, list of centroids) pair
       JavaPairRDD<DataTuple, List<CentroidDataTuple>> dataPointsWithCentroids = datapoints.mapToPair(
                new PairFunction<DataTuple, DataTuple, List<CentroidDataTuple>>() {
            public Tuple2<DataTuple, List<CentroidDataTuple>> call(DataTuple x) {
                return new Tuple2(x, centroids);
            }
        });



        JavaPairRDD <CentroidDataTuple, Tuple2<DataTuple,Integer>> mappedDataPoints = dataPointsWithCentroids.mapToPair(
                new PairFunction<Tuple2<DataTuple, List<CentroidDataTuple>>,
                        CentroidDataTuple, Tuple2<DataTuple,Integer>>() {
            public Tuple2<CentroidDataTuple, Tuple2<DataTuple, Integer>> call(Tuple2<DataTuple, List<CentroidDataTuple>> x) {
                CentroidDataTuple closestCentroid = null;
                double closestDistance = Double.MAX_VALUE;
                for (CentroidDataTuple centroid : x._2){
                    double currentDistance = x._1.computeDistance(centroid);
                    if (currentDistance < closestDistance) {
                        closestCentroid = centroid;
                        closestDistance = currentDistance;
                    }
                }
                return new Tuple2<>(closestCentroid, new Tuple2<>(x._1,1));
            }
        });


        mappedDataPoints.foreach(data -> {
            System.out.println("centroid="+data._1.toString() + " data tuple=" + data._2.toString());
        });

        JavaPairRDD<CentroidDataTuple, Tuple2<DataTuple,Integer>> summedCentroids = mappedDataPoints.reduceByKey(
                new Function2<Tuple2<DataTuple,Integer>, Tuple2<DataTuple,Integer>, Tuple2<DataTuple, Integer>>() {
           public Tuple2<DataTuple, Integer> call(Tuple2<DataTuple,Integer> x, Tuple2<DataTuple,Integer> y) {
               x._1.add(y._1);
               return new Tuple2<>(x._1, x._2 + y._2);
           }
        });

        summedCentroids.foreach(data -> {
            System.out.println("centroid="+data._1.toString() + " summed data tuple=" + data._2.toString());
        });

        JavaRDD<CentroidDataTuple> newCentroids = summedCentroids.map(new Function<Tuple2<CentroidDataTuple, Tuple2<DataTuple, Integer>>,
                CentroidDataTuple>() {
            @Override
            public CentroidDataTuple call(Tuple2<CentroidDataTuple, Tuple2<DataTuple, Integer>> summedDataTuple) {
                summedDataTuple._2._1.divide(summedDataTuple._2._2);
                return new CentroidDataTuple(summedDataTuple._2._1);
            }
        });

        newCentroids.foreach(data -> {
            System.out.println("centroid="+data.toString());
        });
        //the new assignment for centroids
//        centroids = newCentroids.collect();




//        JavaPairRDD<>
//        // ReduceByKey to count the occurrences of each word
//        JavaPairRDD<String, Integer> counts = ones.reduceBy
//Key(new Function2<Integer, Integer, Integer>() {
//            public Integer call(Integer x, Integer y) {
//                return x + y;
//            }
//        });

        // Save the word count back out to a text file, causing evaluation.













    }
}
