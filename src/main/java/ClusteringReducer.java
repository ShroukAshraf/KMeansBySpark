import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class ClusteringReducer {

    private static final double THRESHOLD_CONVERGENCE = 0.1;
//
//    private List<String> centroids = new ArrayList<>();
//
//    @Override
//    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//
//        List<DataTuple> dataTuples = new ArrayList<>();
//        CentroidDataTuple meanCentriod = null;
//        System.out.println(meanCentriod);
//        DistanceCalculator euclideCalculator = new EuclideanCalculator();
//        for (Text value : values) {
//            DataTuple dataTuple = new DataTuple(value.toString(), euclideCalculator);
//            dataTuples.add(dataTuple);
//            if (meanCentriod == null) {
//                meanCentriod = initializeCentroid(dataTuple.getDataPoints().size());
//            }
//            meanCentriod.add(dataTuple);
//        }
//        meanCentriod.divide(dataTuples.size());
//        String centroid = meanCentriod.toString();
//        centroids.add(centroid);
//        for (DataTuple dataTuple : dataTuples) {
//            context.write(new Text(centroid), new Text(dataTuple.toString()));
//        }
//        CentroidDataTuple oldCentroid = new CentroidDataTuple(key.toString(), new EuclideanCalculator());
//        if(meanCentriod.getDifference(oldCentroid) > THRESHOLD_CONVERGENCE) {
//            context.getCounter(KmeansClustering.Iteration.STILL_NOT_CONVERGED).increment(1);
//        }
//    }
//
//    private CentroidDataTuple initializeCentroid(int size) {
//        StringBuilder zeroInput = new StringBuilder();
//        for(int i = 0; i < size; i++) {
//            zeroInput.append("0.0,");
//        }
//        zeroInput.setLength(zeroInput.length() - 1);
//        return new CentroidDataTuple(zeroInput.toString(), new EuclideanCalculator());
//    }
//
//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        super.cleanup(context);
//        Configuration configuration = context.getConfiguration();
//        Path centriodPath = new Path(configuration.get("centroid.new_path"));
//        FileSystem fileSystem = FileSystem.get(configuration);
//        fileSystem.delete(centriodPath, true);
//        FileHelper.writeToFile(centriodPath.toString(), centroids, centroids.size(), fileSystem);
//    }
}