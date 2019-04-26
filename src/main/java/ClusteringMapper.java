import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class ClusteringMapper extends Mapper<Object, Text, Text, Text> {

    private List<CentroidDataTuple> centroids;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration configuration = context.getConfiguration();
        String centroidsPath = new Path(configuration.get("centroid.old_path")).toString();
        FileSystem fileSystem = FileSystem.get(configuration);
        List<String> centroidsStr = FileHelper.readFileLines(centroidsPath, fileSystem);
        centroids = new ArrayList<>();
        for(String centroid : centroidsStr) {
            centroids.add(new CentroidDataTuple(centroid, new EuclideanCalculator()));
        }
        System.out.println("++++++++++++++++++++++++++++>>>>>>>>>>> " + centroids);
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException,
			InterruptedException {
        CentroidDataTuple closestCentroid = null;
        double closestDistance = Double.MAX_VALUE;
        DataTuple dataTuple = new DataTuple(value.toString(), new EuclideanCalculator());
        for(CentroidDataTuple centroid : centroids) {
            double distance = dataTuple.computeDistance(centroid);
            if(distance < closestDistance) {
                closestCentroid = centroid;
                closestDistance = distance;
            } 
        }
        System.out.println("MAPPPPEEEERRRR========================>>>>>>>>>>" + closestCentroid.toString() + " " + value);
		context.write(new Text(closestCentroid.toString()), value);
    }
}