package bike;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

public class Producer {

    private final KafkaProducer<String, String> producer;
    private final HttpClientService httpClient;
    private final String topic;

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

        producer = new KafkaProducer<>(props);
    }

    public void publishBikeData() throws Exception {

        String jsonResponse = httpClient.fetchBikeData();

        JSONArray stations = new JSONArray(jsonResponse);

        System.out.println("Stations found: " + stations.length());

        for (int i = 0; i < stations.length(); i++) {

            JSONObject station = stations.getJSONObject(i);

            String stationId = station.optString("id");


            String id = stationId.startsWith("BikePoints_")
                    ? stationId.substring("BikePoints_".length())
                    : stationId;
            String name = station.optString("commonName");
            String terminalName = station.optString("terminalName");

            double latitude = station.optDouble("lat");
            double longitude = station.optDouble("lon");

            boolean installed = station.optBoolean("installed");
            boolean locked = station.optBoolean("locked");

            int bikesAvailable = 0;
            int standardBikes = 0;
            int eBikes = 0;
            int emptyDocks = 0;
            int totalDocks = 0;

            JSONArray properties = station.optJSONArray("additionalProperties");

            if (properties != null) {

                for (int j = 0; j < properties.length(); j++) {

                    JSONObject property = properties.getJSONObject(j);

                    String key = property.optString("key");
                    String value = property.optString("value", "0");

                    switch (key) {

                        case "NbBikes":
                            bikesAvailable = Integer.parseInt(value);
                            break;

                        case "NbStandardBikes":
                            standardBikes = Integer.parseInt(value);
                            break;

                        case "NbEBikes":
                            eBikes = Integer.parseInt(value);
                            break;

                        case "NbEmptyDocks":
                            emptyDocks = Integer.parseInt(value);
                            break;

                        case "NbDocks":
                            totalDocks = Integer.parseInt(value);
                            break;
                    }
                }
            }

            JSONObject event = new JSONObject();

            event.put("stationId", id);
            event.put("stationName", name);
            event.put("terminalName", terminalName);
            event.put("latitude", latitude);
            event.put("longitude", longitude);
            event.put("installed", installed);
            event.put("locked", locked);
            event.put("bikesAvailable", bikesAvailable);
            event.put("standardBikes", standardBikes);
            event.put("eBikes", eBikes);
            event.put("emptyDocks", emptyDocks);
            event.put("totalDocks", totalDocks);
            event.put("timestamp", System.currentTimeMillis());

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, id, event.toString());

            RecordMetadata metadata = producer.send(record).get();

            System.out.printf(
                    "Published %s -> topic=%s partition=%d offset=%d%n",
                    id,
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());

            Thread.sleep(100);
        }

        producer.flush();

        System.out.println("Finished publishing bike stations.");
    }

    public void close() {

        producer.close();

    }
}