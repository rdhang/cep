package home.cep;

import home.avro.model.Alert;
import home.avro.model.Event;
import home.avro.model.Threshold;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BiasDetectionJob {

	static final String BOOTSTRAP_SERVER = "localhost:9092";
	static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().enableCheckpointing(1000);





		KafkaSource<Event> eventSource = KafkaSource.<Event>builder()
				.setBootstrapServers(BOOTSTRAP_SERVER)
				.setTopics("model.bias")
				.setGroupId("bias-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				//.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ConfluentRegistryAvroDeserializationSchema.forSpecific(Event.class, SCHEMA_REGISTRY_URL)))
				.setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forSpecific(Event.class, SCHEMA_REGISTRY_URL))
				.build();

		DataStream<Event> eventStream= env.fromSource(eventSource, WatermarkStrategy.noWatermarks(), "Event Source")
				.keyBy(Event::getName);


		KafkaSource<Threshold> thresholdSource = KafkaSource.<Threshold>builder()
				.setBootstrapServers(BOOTSTRAP_SERVER)
				.setTopics("model.bias.threshold")
				.setGroupId("threshold-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				//.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ConfluentRegistryAvroDeserializationSchema.forSpecific(Event.class, SCHEMA_REGISTRY_URL)))
				.setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forSpecific(Threshold.class, SCHEMA_REGISTRY_URL))
				.build();

		DataStream<Threshold> thresholdStream= env.fromSource(thresholdSource, WatermarkStrategy.noWatermarks(), "Threshold Source")
				.keyBy(Threshold::getName);


		DataStream<Alert> alerts = eventStream
				.connect(thresholdStream)
				.flatMap(new BiasDetector())
				.name("bias-detector");

		alerts.print();

		alerts.sinkTo(
				KafkaSink.<Alert>builder()
						.setBootstrapServers(BOOTSTRAP_SERVER)
						.setRecordSerializer(
								KafkaRecordSerializationSchema.builder()
										.setTopic("alerts.topic")
										.setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema.forSpecific(Alert.class, "", SCHEMA_REGISTRY_URL))
										.build())
						.build());

		env.execute("Bias Detection");
	}
}