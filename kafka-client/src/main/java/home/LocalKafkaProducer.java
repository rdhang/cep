package home;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;


public class LocalKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(LocalKafkaProducer.class);

    public static void main(String[] args) throws Exception {

        InputStream input = LocalKafkaProducer.class.getClassLoader().getResourceAsStream("config.properties");

        //Assign topicName to string variable
        String topicName = "events.topic";

        Properties properties = new Properties();
        properties.load(input);

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String key = UUID.randomUUID().toString();
            String value = Integer.toString(i);
            producer.send(new ProducerRecord<>(topicName, key, value));
        }

        log.info("Messages sent successfully");

        producer.close();
    }
}