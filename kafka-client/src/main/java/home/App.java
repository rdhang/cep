package home;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import home.avro.model.Event;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    static final Logger logger = LoggerFactory.getLogger(App.class);

    private static final String TOPIC = "model.bias";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    //
    public static void main(String[] args) {
        //Thread consumerThread = new Thread(App::consume);
       //consumerThread.start();

        Thread producerThread = new Thread(App::produce);
       producerThread.start();
    }

    private static void produce() {
        // Create configuration options for our producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        // We configure the serializer to describe the format in which we want to produce data into our Kafka cluster
        // Avro properties
        //props.setProperty("schema.registry.url", REGISTRY);
        //props.setProperty("specific.avro.reader", "true");

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        // wait until we get 10 messages before writing
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "10");
        // no matter what happens, write all pending messages
        // every 2 seconds
        props.put(ProducerConfig.LINGER_MS_CONFIG, "2000");

        List<Event> events;
        try {
            events = new DataIngest().fetch();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Since we need to close our producer, we can use the try-with-resources statement to
        // create a new producer

        try(KafkaProducer<String, Event> producer = new KafkaProducer<>(props)) {

            // here, we run an infinite loop sent a message to the cluster every second



            while (true) {

                for (Event event : events) {

//                    long now = LocalDateTime.now().atZone(ZoneId.of("America/New_York")).toInstant().toEpochMilli();
//                    Event event = Event.newBuilder().setName("bias").setTimestamp(now).setData(24L).build();

                    ProducerRecord<String, Event> producerRecord = new ProducerRecord<>("model.bias", event.getName(), event);

                    producer.send(producerRecord, (recordMetadata, e) -> {
                        if (e == null) {
                            logger.info(recordMetadata.toString());
                        } else {
                            logger.error(e.getMessage());
                        }
                    });

                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            logger.error("Could not start producer: " + e);
        }
    }

    private static void consume() {
        // Create configuration options for our consumer
        Properties props = getProperties();

        // Since we need to close our consumer, we can use the try-with-resources statement to
        // create it
        try (KafkaConsumer<String, Event> consumer = new KafkaConsumer<>(props)) {
            // Subscribe this consumer to the same topic that we wrote messages to earlier
            consumer.subscribe(List.of(TOPIC));
            // run an infinite loop where we consume and print new messages to the topic
            while (true) {
                // The consumer.poll method checks and waits for any new messages to arrive for the subscribed topic
                // in case there are no messages for the duration specified in the argument (1000 ms
                // in this case), it returns an empty list
                ConsumerRecords<String, Event> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Event> record : records) {
                    logger.info("received message: " + record.value().getName());
                }
            }
        }
    }

    @NotNull
    private static Properties getProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

        // Every time we consume a message from kafka, we need to "commit" - that is, acknowledge
        // receipts of the messages. We can set up an auto-commit at regular intervals, so that
        // this is taken care of in the background
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "150");
        props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "2500");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
