import java.io.Serializable;

public interface DistanceCalculator extends Serializable {
    double getDistance(DataTuple firstDataTuple, DataTuple secondDataTuple);
}