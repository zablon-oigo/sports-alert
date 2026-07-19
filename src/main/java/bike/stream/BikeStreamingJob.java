package bike.stream;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import bike.BikeStation;

public class BikeStreamingJob {

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = "localhost:9092";
        final String schemaRegistryUrl = "http://localhost:8081";
        final String topic = "bike.analytics";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<BikeStation> source = KafkaSource.<BikeStation>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("bike-stream-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                    ConfluentRegistryAvroDeserializationSchema.forSpecific(
                        BikeStation.class, schemaRegistryUrl
                    )
                )
                .build();

        DataStream<BikeStation> stream = env.fromSource(
                source,
                WatermarkStrategy.<BikeStation>noWatermarks(),
                "Bike Stations Source"
        )
        
        .returns(TypeInformation.of(BikeStation.class));

        stream
                .keyBy(BikeStation::getStationId)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(30)))
                .aggregate(new AverageBikeAggregate())
                .print("Average bikes per station: ");

        env.execute("Bike Station Average Streaming Job");
    }
}