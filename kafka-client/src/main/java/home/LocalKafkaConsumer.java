package home;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class LocalKafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(LocalKafkaConsumer.class);

    public static void main(String[] args) throws Exception {

        String topic = "events.topic";

        InputStream input = LocalKafkaConsumer.class.getClassLoader().getResourceAsStream("config.properties");

        // create consumer configs
        Properties properties = new Properties();
        properties.load(input);

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
        }));

        try {
            // subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singletonList(topic));

            // poll for new data
            while (true) {
                long pollInterval = Long.parseLong(properties.getProperty("poll.interval"));
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(pollInterval));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                }
            }

        } catch (WakeupException e) {
            log.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            consumer.close(); // this will also commit the offsets if need be.
            log.info("The consumer is now gracefully closed.");
        }
    }
}