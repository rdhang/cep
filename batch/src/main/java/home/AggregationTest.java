package home;

import lombok.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.log4j.Logger;

import java.util.Random;

public class AggregationTest {

    private static final Logger LOG = Logger.getLogger(AggregationTest.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<MyEvent> events = env.addSource(new MySource());

        events.keyBy(myEvent -> myEvent.variant)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .aggregate(new AverageAggregator())
                .print();

        env.execute();
    }

    /**
     * Aggregation function for average.
     */
    public static class AverageAggregator implements AggregateFunction<MyEvent, MyAverage, Tuple2<MyAverage, Double>> {

        @Override
        public MyAverage createAccumulator() {
            return new MyAverage();
        }

        @Override
        public MyAverage add(MyEvent myEvent, MyAverage myAverage) {
            LOG.debug(myAverage.variant + " " + myEvent);
            myAverage.variant = myEvent.variant;
            myAverage.count = myAverage.count + 1;
            myAverage.sum = myAverage.sum + myEvent.cev;
            return myAverage;
        }

        @Override
        public Tuple2<MyAverage, Double> getResult(MyAverage myAverage) {
            return new Tuple2<>(myAverage, myAverage.sum / myAverage.count);
        }

        @Override
        public MyAverage merge(MyAverage myAverage, MyAverage acc1) {
            myAverage.sum = myAverage.sum + acc1.sum;
            myAverage.count = myAverage.count + acc1.count;
            return myAverage;
        }
    }

    /**
     * Produce never ending stream of fake updates.
     */
    public static class MySource extends RichSourceFunction<MyEvent> {

        private boolean running = true;

        private final String[] variants = new String[] { "var1", "var2" };
        private final String[] products = new String[] { "prodfoo", "prodBAR", "prod-baz" };
        private final Random random = new Random();

        @Override
        public void run(SourceContext<MyEvent> sourceContext) throws Exception {
            while (running) {
                String variant = variants[random.nextInt(variants.length)];
                String product = products[random.nextInt(products.length)];
                Double value = random.nextDouble() * 10;
                sourceContext.collect(new MyEvent(variant, product, value));
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * Immutable update event.
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    @EqualsAndHashCode
    @Builder
    public static class MyEvent {
        public String variant;
        public String product;
        public Double cev;

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    @EqualsAndHashCode
    @Builder
    public static class MyAverage {

        public String variant;
        public Integer count = 0;
        public Double sum = 0d;
    }

}
