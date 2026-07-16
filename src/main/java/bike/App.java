package bike;

import java.io.File;

import org.apache.avro.Schema;

public class App {

    private static final String BOOTSTRAP = "localhost:29092";
    private static final String TOPIC = "bike.analytics";

    public static void main(String[] args) {
        try {

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(new File("src/main/avro/BikeStation.avsc"));

            Producer producer = new Producer(BOOTSTRAP, TOPIC, schema);

            producer.publishBikeData();
            System.out.println("Finished publishing bike stations.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}