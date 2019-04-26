import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class Evaluator {

    public Evaluator() {

    }

	private static Map<DataTuple, String> readGroundTruthData(String groundTruthPath, FileSystem fileSystem) {
		// Populate map of <data_tuple, label> from ground truth dataset
		Map<DataTuple, String> groundTruthMap = new HashMap<>();
		try {
			InputStreamReader inputStreamReader = new InputStreamReader(fileSystem.open(new Path(groundTruthPath)));
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			String line = bufferedReader.readLine();
			while (line != null) {
				String features = line.substring(0, line.lastIndexOf(','));
				String label = line.substring(line.lastIndexOf(',') + 1);				
				// System.out.println("Features: " + features + "  ----  " + label);
				groundTruthMap.put(new DataTuple(features, new EuclideanCalculator()), label);
				line = bufferedReader.readLine();
			}
			bufferedReader.close();
		} catch(IOException ioException) {
			System.out.println("Error reading " + groundTruthPath);
		}
		return groundTruthMap;
	}

	private static Map<CentroidDataTuple, List<DataTuple>> readOutputClusters(String outputPath, FileSystem fileSystem) {
		Map<CentroidDataTuple, List<DataTuple>> clusters = new HashMap<>();
		try {
			InputStreamReader inputStreamReader = new InputStreamReader(fileSystem.open(new Path(outputPath)));
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			String line = bufferedReader.readLine();
			while (line != null) {
				String[] splits = line.split("\\t");
				CentroidDataTuple centroid = new CentroidDataTuple(splits[0], new EuclideanCalculator());
				DataTuple sample = new DataTuple(splits[1], new EuclideanCalculator());
				if (!clusters.containsKey(centroid)) {
					clusters.put(centroid, new ArrayList<>());
				}
				clusters.get(centroid).add(sample);
				line = bufferedReader.readLine();
			}
			bufferedReader.close();
		} catch(IOException ioException) {
			System.out.println("Error reading " + outputPath);
		}
		return clusters;
	}

	private static String majorityVote(List<DataTuple> tuples, Map<DataTuple, String> groundTruthMap) {
		Map<String, Integer> labelsCntMap = new HashMap<>();
		for (DataTuple tuple : tuples) {
			String label = groundTruthMap.get(tuple);
			// System.out.println("Current label: " + label);
			if (!labelsCntMap.containsKey(label)) {
				labelsCntMap.put(label, 1);
			} else {
				labelsCntMap.put(label, labelsCntMap.get(label) + 1);
			}
		}
		return labelsCntMap.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
	}
	
    // Evaluates the clustering accuracy of the given result of the clustering algo compared to the ground truth
    public static void evaluate(String groundTruthPath, String outputPath, FileSystem fileSystem) {
		
		Map<DataTuple, String> groundTruthMap = readGroundTruthData(groundTruthPath, fileSystem);
		Map<CentroidDataTuple, List<DataTuple>> clusters = readOutputClusters(outputPath, fileSystem);

		int totalSamples = 0;
		int totalMisclassified = 0;
		for (Map.Entry<CentroidDataTuple, List<DataTuple>> entry : clusters.entrySet()) {
			// Majority vote to determine cluster label
			String clusterLabel = majorityVote(entry.getValue(), groundTruthMap);
			
			System.out.println("=================== Cluster label: " + clusterLabel + " ===================\n");
			// Count misclassified samples in this cluster
			for (DataTuple tuple : entry.getValue()) {
				if (!groundTruthMap.get(tuple).equals(clusterLabel)) {
					totalMisclassified++;
				}
				totalSamples++;
			}
		}

		// Report clustering error
		System.out.println("Misclassified samples = " + totalMisclassified);
		System.out.println("Total samples in the dataset = " + totalSamples);
		System.out.println("Clustering accuracy = " + (((double) (totalSamples - totalMisclassified) ) / totalSamples));
    }
}