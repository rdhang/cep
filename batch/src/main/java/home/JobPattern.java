package home;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class JobPattern {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        ParameterTool params = ParameterTool.fromArgs(args);

//        CsvReaderFormat<Event> csvFormat = CsvReaderFormat.forPojo(Event.class);
//        FileSource<Event> source =
//                FileSource.forRecordStreamFormat(csvFormat, new Path(params.get("input")))
//                        .monitorContinuously(Duration.of(5, SECONDS))
//                        .build();

        DataStream<Sensor> inputStream = env.addSource(new TemperatureSensor())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Sensor>() {
                    @Override
                    public long extractAscendingTimestamp(Sensor sensor) {
                        return sensor.getTimestamp();
                    }
                });

        DataStream<Double> temps = inputStream.map(e -> { return e.getTemp(); });

//        DataStreamSource<Event> eventStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "File");
//
//        Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
//                .where(SimpleCondition.of(event -> event.getHumidity() > 80.0F))
//                .next("middle")
//                .subtype(SubEvent.class)
//                .where(SimpleCondition.of(subEvent -> subEvent.getVolume() >= 10.0))
//                .followedBy("end")
//                .where(SimpleCondition.of(event -> event.getName().equals("end")));
//
//        PatternStream<Event> patternStream = CEP.pattern(eventStream, pattern);
//
//        DataStream<Alert> result = patternStream.process(
//                new PatternProcessFunction<Event, Alert>() {
//                    @Override
//                    public void processMatch(
//                            Map<String, List<Event>> pattern,
//                            Context ctx,
//                            Collector<Alert> out) throws Exception {
//                        out.collect(createAlertFrom(pattern));
//                    }
//                });
//

        Pattern<Sensor, ?> highTempPattern = Pattern.<Sensor>begin("start")
                        .where(new SimpleCondition<Sensor>() {
                            @Override
                            public boolean filter(Sensor sensor) throws Exception {
                                return sensor.getTemp() > 60;
                            }
                        });

        DataStream<Sensor> result = CEP.pattern(inputStream.keyBy(new KeySelector<Sensor, Integer>() {
            @Override
            public Integer getKey(Sensor sensor) throws Exception {
                return sensor.getDeviceId();
            }
        }), highTempPattern).process(new PatternProcessFunction<Sensor, Sensor>() {
            @Override
            public void processMatch(Map<String, List<Sensor>> map, Context context, Collector<Sensor> collector) throws Exception {
                collector.collect(map.get("start").get(0));
            }
        });

        SingleOutputStreamOperator<Tuple3<Integer, Long, Integer>> aggregatedMatch = result.keyBy(new KeySelector<Sensor, Integer>() {
                    @Override
                    public Integer getKey(Sensor sensor) throws Exception {
                        return sensor.getDeviceId();
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .aggregate(new AggregateFunction<Sensor, Tuple3<Integer, Long, Integer>, Tuple3<Integer, Long, Integer>>() {
                    @Override
                    public Tuple3<Integer, Long, Integer> createAccumulator() {
                        Tuple3<Integer, Long, Integer> acc = new Tuple3<>();
                        acc.f0 = -1;
                        return acc;
                    }

                    @Override
                    public Tuple3<Integer, Long, Integer> add(Sensor sensor, Tuple3<Integer, Long, Integer> integerLongtuple2) {
                        if(integerLongtuple2.f0 == -1){
                            integerLongtuple2.f0 = sensor.getDeviceId();
                            integerLongtuple2.f1 = sensor.getTimestamp();
                            integerLongtuple2.f2 = 0;
                        }
                        integerLongtuple2.f2++;
                        return integerLongtuple2;
                    }

                    @Override
                    public Tuple3<Integer, Long, Integer> getResult(Tuple3<Integer, Long, Integer> integerLongtuple2) {
                        return integerLongtuple2;
                    }

                    @Override
                    public Tuple3<Integer, Long, Integer> merge(Tuple3<Integer, Long, Integer> integerLongtuple2, Tuple3<Integer, Long, Integer> acc1) {
                        acc1.f2 += integerLongtuple2.f2;
                        return acc1;
                    }
                });

        DataStream<Tuple2<Integer, Long>> aggregatedResult = CEP.pattern(aggregatedMatch,
                Pattern.<Tuple3<Integer, Long, Integer>>begin("aggs")
                        .where(new SimpleCondition<Tuple3<Integer, Long, Integer>>() {
                            @Override
                            public boolean filter(Tuple3<Integer, Long, Integer> integerLongTuple3) throws Exception {
                                return integerLongTuple3.f2 > 4;
                            }
                        })).process(new PatternProcessFunction<Tuple3<Integer, Long, Integer>, Tuple2<Integer, Long>>() {
            @Override
            public void processMatch(Map<String, List<Tuple3<Integer, Long, Integer>>> map, Context context, Collector<Tuple2<Integer, Long>> collector) throws Exception {
                Tuple2<Integer, Long> result = new Tuple2<>();
                result.f0 = map.get("aggs").get(0).f0;
                result.f1 = map.get("aggs").get(0).f1;
                collector.collect(result);
            }
        });

        result.print();

        env.execute("cep");
    }


}
