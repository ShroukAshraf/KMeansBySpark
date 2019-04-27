public class CentroidDataTuple extends DataTuple {

    public CentroidDataTuple(String dataLine, DistanceCalculator distanceCalculator) {
        super(dataLine, distanceCalculator);
    }

    public CentroidDataTuple (DataTuple dataTuple) {
        super(dataTuple);
    }

    public double getDifference(CentroidDataTuple otherCentroid) {
        Double difference = Double.MAX_VALUE;
        if(otherCentroid.getDataPoints() == null || otherCentroid.getDataPoints().size() != dataPoints.size()) {
            return difference;
        }
        difference = 0.0;
        for(int i = 0; i < dataPoints.size(); i++) {
            difference += Math.abs(dataPoints.get(i) - otherCentroid.getDataPoints().get(i));
        }
        return difference;
    }

}