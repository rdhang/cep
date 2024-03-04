package sandbox;

import home.avro.model.Alert;
import home.avro.model.Event;
import home.avro.model.Threshold;
import home.cep.BiasDetector;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.DynamicProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import sandbox.functions.ModelCountAggregation;
import sandbox.model.SystemEvent;
import sandbox.util.JsonPojoSerializer;

import javax.annotation.Nullable;
import java.io.File;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;
import java.util.function.Function;

public class BiasDetectionJob {

	private static final Logger LOG = Logger.getLogger(BiasDetectionJob.class);
	static final String BOOTSTRAP_SERVER = "localhost:9092";
	static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";


	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
				//.setRuntimeMode(RuntimeExecutionMode.BATCH)
				.enableCheckpointing(1000);





//		SerializableFunction<CsvMapper, CsvSchema> schemaGenerator =
//				mapper -> mapper.schemaFor(Car.class).withHeader().withoutQuoteChar().withColumnSeparator(',');
//
//		CsvReaderFormat<Car> csvFormat =
//				CsvReaderFormat.forSchema(CsvMapper::new, schemaGenerator, TypeInformation.of(Car.class));
//
//		FileSource<Car> source = FileSource
//				.forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File("/opt/data/Car_sales.csv")))
//				.monitorContinuously(Duration.ofMillis(5))
//				.build();
//
//		DataStream<Car> transactionDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
//
//		DataStream<Tuple2<String, Integer>> countsByManufacturer =  transactionDataStream
//				.keyBy(Car::getManufacturer)
//						.window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(5000)))
//								.aggregate(new ModelCountAggregation());
//
//		countsByManufacturer.print();
//

		final FileSource<String> systemEventSource =
				FileSource.forRecordStreamFormat(new TextLineInputFormat(),
								new Path("/opt/data/system-events") )
						.monitorContinuously(Duration.ofSeconds(5L))
						.build();

		final DataStream<String> stream =
				env.socketTextStream("localhost", 9999, "\n");

			DataStream<SystemEvent> systemEventStream = stream.map((MapFunction<String, SystemEvent>) s -> {
                CsvMapper mapper = new CsvMapper();
				mapper.registerModule(new JavaTimeModule());
                CsvSchema schema = mapper.schemaFor(SystemEvent.class).withoutHeader();

                return mapper.readerFor(SystemEvent.class).with(schema).readValue(s);
            });

		Properties config = new Properties();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPojoSerializer.class);

		KafkaSink<SystemEvent> sink = KafkaSink.<SystemEvent>builder()
				.setKafkaProducerConfig(config)
								.setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<>()
										.setValueSerializationSchema(new JsonSerializationSchema<SystemEvent>())
										.setTopic("system-events")
										.build())
				.build();

		systemEventStream.sinkTo(sink);

		KafkaSource<SystemEvent> eventSource = KafkaSource.<SystemEvent>builder()
				.setBootstrapServers(BOOTSTRAP_SERVER)
				.setTopics("system-events")
				.setGroupId("events-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				//.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ConfluentRegistryAvroDeserializationSchema.forSpecific(Event.class, SCHEMA_REGISTRY_URL)))
				.setValueOnlyDeserializer(new JsonDeserializationSchema<>(SystemEvent.class))// ConfluentRegistryAvroDeserializationSchema.forSpecific(Event.class, SCHEMA_REGISTRY_URL))
				.build();



		DataStream<SystemEvent> eventStream= env.fromSource(eventSource, WatermarkStrategy.noWatermarks(), "Event Source");
		eventStream.print();

//
//
//		KafkaSource<Threshold> thresholdSource = KafkaSource.<Threshold>builder()
//				.setBootstrapServers(BOOTSTRAP_SERVER)
//				.setTopics("model.bias.threshold")
//				.setGroupId("threshold-group")
//				.setStartingOffsets(OffsetsInitializer.earliest())
//				//.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ConfluentRegistryAvroDeserializationSchema.forSpecific(Event.class, SCHEMA_REGISTRY_URL)))
//				.setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forSpecific(Threshold.class, SCHEMA_REGISTRY_URL))
//				.build();
//
//		DataStream<Threshold> thresholdStream= env.fromSource(thresholdSource, WatermarkStrategy.noWatermarks(), "Threshold Source")
//				.keyBy(Threshold::getName);
//
//
//		DataStream<Alert> alerts = eventStream
//				.connect(thresholdStream)
//				.flatMap(new BiasDetector())
//				.name("bias-detector");
//
//		alerts.print();
//
//		alerts.sinkTo(
//				KafkaSink.<Alert>builder()
//						.setBootstrapServers(BOOTSTRAP_SERVER)
//						.setRecordSerializer(
//								KafkaRecordSerializationSchema.builder()
//										.setTopic("alerts.topic")
//										.setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema.forSpecific(Alert.class, "", SCHEMA_REGISTRY_URL))
//										.build())
//						.build());

		env.execute("Bias Detection");
	}
}