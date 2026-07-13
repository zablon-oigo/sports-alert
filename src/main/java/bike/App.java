package bike;

public class App {

    private static final String BOOTSTRAP = "localhost:29092";
    private static final String TOPIC = "bike.analytics";

    public static void main(String[] args) {

        HttpClientService http = new HttpClientService();

        Producer producer = new Producer(BOOTSTRAP, TOPIC);

        try {

            String json = http.getBikeStations();

            producer.send("bikepoints", json);

            System.out.println("Bike data published to Kafka.");

        }catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            producer.close();
        }
    }
}