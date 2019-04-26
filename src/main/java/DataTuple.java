import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.*;
import java.util.stream.Collectors;


public class DataTuple implements Serializable {
    
    protected List<Double> dataPoints;
    protected DistanceCalculator distanceCalculator;

    public  DataTuple (DataTuple tuple) {
        this.dataPoints = new ArrayList<>(tuple.dataPoints);
        this.distanceCalculator = tuple.distanceCalculator;
    }
    public DataTuple(String dataLine, DistanceCalculator distanceCalculator) {
        dataPoints = getDataPoints(dataLine);
        this.distanceCalculator = distanceCalculator;
    }

    private List<Double> getDataPoints(String dataLine) {
        String[] strDataPoints = dataLine.split(",");
        List<Double> doubleDataPoints = new ArrayList<>();
        for(String dataPoint : strDataPoints) {
            doubleDataPoints.add(Double.valueOf(dataPoint));
        } 
        return doubleDataPoints;
    }

    public List<Double> getDataPoints() {
        return dataPoints;
    }

    public double computeDistance(DataTuple otherTuple) {
        return distanceCalculator.getDistance(this, otherTuple);
    }

    public void add(DataTuple otherTuple) {
        for(int i = 0; i < dataPoints.size(); i++) {
            dataPoints.set(i, dataPoints.get(i) + otherTuple.getDataPoints().get(i));
        }
    }

    public void divide(double value) {
        for(int i = 0; i < dataPoints.size(); i++) {
            dataPoints.set(i, dataPoints.get(i) / value);
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(Double dataPoint : dataPoints) {
            sb.append(dataPoint.toString() + ",");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
     
   @Override
   public boolean equals(Object o) {
       if (this == o) return true;
       if (!(o instanceof DataTuple)) return false;
       DataTuple dataTuple = (DataTuple) o;
       return Objects.equals(dataPoints.stream().collect(Collectors.toSet()), dataTuple.dataPoints.stream().collect(Collectors.toSet()));
   }

   @Override
   public int hashCode() {
       return Objects.hash(dataPoints.stream().collect(Collectors.toSet()));
}
}