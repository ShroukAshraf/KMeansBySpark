public class EuclideanCalculator implements DistanceCalculator {

    public double getDistance(DataTuple firstDataTuple, DataTuple secondDataTuple) { 
        double euclideanDistance = 0.0;
        for(int i = 0; i < firstDataTuple.getDataPoints().size(); i++) {
            double num1 = firstDataTuple.getDataPoints().get(i);
            double num2 = secondDataTuple.getDataPoints().get(i);
            euclideanDistance += Math.pow(num1 - num2, 2);
        }
        return Math.sqrt(euclideanDistance);
    }

}