package bike.stream;

import org.apache.flink.api.common.functions.AggregateFunction;

import bike.BikeStation;

public class AverageBikeAggregate implements AggregateFunction<BikeStation, AverageAccumulator, Double> {

    @Override
    public AverageAccumulator createAccumulator() {
        return new AverageAccumulator();
    }

    @Override
    public AverageAccumulator add(BikeStation value, AverageAccumulator accumulator) {

        accumulator.add(value.getBikesAvailable());   
        return accumulator;
    }

    @Override
    public Double getResult(AverageAccumulator accumulator) {
        return accumulator.getAverage();
    }

    @Override
    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
        a.merge(b);
        return a;
    }
}


class AverageAccumulator {
    private long count = 0;
    private double sum = 0.0;

    public void add(int bikes) {
        sum += bikes;
        count++;
    }

    public double getAverage() {
        return count == 0 ? 0.0 : sum / count;
    }

    public void merge(AverageAccumulator other) {
        this.sum += other.sum;
        this.count += other.count;
    }
}