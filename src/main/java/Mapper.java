import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class Mapper implements PairFunction<DataTuple,
        CentroidDataTuple, Tuple2<DataTuple,Integer>> {

    private List<CentroidDataTuple> centroids;
    Mapper(List<CentroidDataTuple> centroids) {
        this.centroids = centroids;
    }
    @Override
    public Tuple2<CentroidDataTuple, Tuple2<DataTuple, Integer>> call(DataTuple dataTuple) throws Exception {
        CentroidDataTuple closestCentroid = null;
        double closestDistance = Double.MAX_VALUE;
        for (CentroidDataTuple centroid : centroids){
            double currentDistance = dataTuple.computeDistance(centroid);
            if (currentDistance < closestDistance) {
                closestCentroid = centroid;
                closestDistance = currentDistance;
            }
        }
        return new Tuple2<>(closestCentroid, new Tuple2<>(dataTuple, 1));
    }
}
