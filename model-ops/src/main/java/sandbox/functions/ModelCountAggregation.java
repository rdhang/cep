package sandbox.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import sandbox.model.Car;

public class ModelCountAggregation implements AggregateFunction<Car, Tuple2<String, Integer>, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> createAccumulator() {
        return new Tuple2<>("", 0);
    }

    @Override
    public Tuple2<String, Integer> add(Car car, Tuple2<String, Integer> acc) {
        return new Tuple2<>(car.manufacturer, acc.f1 + 1);
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> acc) {
        return new Tuple2<>(acc.f0, acc.f1);
    }

    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> acc1) {
        return null;
    }
}
