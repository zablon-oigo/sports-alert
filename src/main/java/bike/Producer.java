package bike;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final HttpClientService httpClient;

    public Producer(String bootstrapServers, String topic) {

        this.topic = topic;
        this.httpClient = new HttpClientService();

        Properties props = new Properties();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(
                ProducerConfig.ACKS_CONFIG,
                "all");

        producer = new KafkaProducer<>(props);
    }

    public void publishBikeData() {

        String xml = httpClient.fetchBikeData();

        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, xml);

        producer.send(record, (metadata, exception) -> {

            if (exception == null) {

                System.out.println(
                        "Message sent successfully");
                System.out.println(
                        "Topic: " + metadata.topic());
                System.out.println(
                        "Partition: " + metadata.partition());
                System.out.println(
                        "Offset: " + metadata.offset());

            } else {

                System.err.println("Failed to send message.");
                exception.printStackTrace();

            }

        });

        producer.flush();
    }

    public void close() {

        producer.flush();
        producer.close();

    }
}