package bike;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class Producer {

    private final KafkaProducer<String, GenericRecord> producer;
    private final HttpClientService httpClient;
    private final String topic;
    private final Schema schema;

    public Producer(String bootstrapServers, String topic, Schema schema) {
        this.topic = topic;
        this.httpClient = new HttpClientService();
        this.schema = schema;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        this.producer = new KafkaProducer<>(props);
    }

    public void publishBikeData() throws Exception {
        String jsonResponse = httpClient.fetchBikeData();
        JSONArray stations = new JSONArray(jsonResponse);

        System.out.println("Stations found: " + stations.length());

        for (int i = 0; i < stations.length(); i++) {
            JSONObject station = stations.getJSONObject(i);

            String stationIdRaw = station.optString("id");
            String stationId = stationIdRaw.startsWith("BikePoints_")
                    ? stationIdRaw.substring("BikePoints_".length())
                    : stationIdRaw;

            String name = station.optString("commonName");
            String terminalName = station.optString("terminalName");

            double latitude = station.optDouble("lat", 0.0);
            double longitude = station.optDouble("lon", 0.0);

            int bikesAvailable = 0, standardBikes = 0, eBikes = 0, emptyDocks = 0, totalDocks = 0;

            JSONArray properties = station.optJSONArray("additionalProperties");
            if (properties != null) {
                for (int j = 0; j < properties.length(); j++) {
                    JSONObject prop = properties.getJSONObject(j);
                    String key = prop.optString("key");
                    String val = prop.optString("value", "0");

                    if ("NbBikes".equals(key)) {
                        bikesAvailable = parseIntSafe(val);
                    } else if ("NbStandardBikes".equals(key)) {
                        standardBikes = parseIntSafe(val);
                    } else if ("NbEBikes".equals(key)) {
                        eBikes = parseIntSafe(val);
                    } else if ("NbEmptyDocks".equals(key)) {
                        emptyDocks = parseIntSafe(val);
                    } else if ("NbDocks".equals(key)) {
                        totalDocks = parseIntSafe(val);
                    }
                }
            }

            GenericRecord record = new GenericData.Record(schema);
            record.put("stationId", stationId);
            record.put("stationName", name);
            record.put("latitude", latitude);
            record.put("longitude", longitude);
            record.put("bikesAvailable", bikesAvailable);
            record.put("emptyDocks", emptyDocks);
            record.put("totalDocks", totalDocks);
            record.put("eBikes", eBikes);
            record.put("timestamp", String.valueOf(System.currentTimeMillis()));

            ProducerRecord<String, GenericRecord> producerRecord =
                    new ProducerRecord<>(topic, stationId, record);

            RecordMetadata metadata = producer.send(producerRecord).get();

            System.out.printf("Published %s -> topic=%s partition=%d offset=%d%n",
                    stationId, metadata.topic(), metadata.partition(), metadata.offset());

            Thread.sleep(100);
        }

        producer.flush();
        System.out.println("Finished publishing bike stations.");
    }

    private int parseIntSafe(String value) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return 0;
        }
    }

    public void close() {
        producer.close();
    }
}