package home.cep;

import home.avro.model.Alert;
import home.avro.model.Event;
import home.avro.model.Threshold;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class BiasDetector extends RichCoFlatMapFunction<Event, Threshold, Alert> {

	private static final long serialVersionUID = 1L;

	private transient ValueState<Double> upperBoundState;
	private transient ValueState<Double> lowerBoundState;


	@Override
	public void open(Configuration config) {
		upperBoundState = getRuntimeContext().getState(new ValueStateDescriptor<>("upper-bound", Double.class));
		lowerBoundState = getRuntimeContext().getState(new ValueStateDescriptor<>("lower-bound", Double.class));
	}

	@Override
	public void flatMap2(Threshold threshold, Collector<Alert> collector) throws Exception {
		upperBoundState.update(Double.valueOf(threshold.getData().get("UPPER_BOUND")));
		lowerBoundState.update(Double.valueOf(threshold.getData().get("LOWER_BOUND")));
	}

	@Override
	public void flatMap1(Event event, Collector<Alert> collector) throws Exception {

		Alert alert = new Alert();
		alert.setId(UUID.randomUUID().toString());
		alert.setName(event.getName());
		alert.setTimestamp(Instant.now().toEpochMilli());

		Double upperBound = upperBoundState.value();
		if (upperBound != null && Double.parseDouble(event.getData().get("VALUE")) >= upperBound) {
			alert.setData( Map.of("STATUS", "ABOVE_THRESHOLD") );
		}

		Double lowerBound = lowerBoundState.value();
		if (lowerBound != null && Double.parseDouble(event.getData().get("VALUE")) <= lowerBound) {
			alert.setData( Map.of("STATUS", "BELOW_THRESHOLD") );
		}

		collector.collect(alert);
	}
}