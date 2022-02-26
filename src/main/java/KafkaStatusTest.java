import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;

import java.util.Collections;
import java.util.Properties;

/** Kafka Streaming Job for test. */
public class KafkaStatusTest {
    public static void main(String[] args) {

        String topic = "test.db_gb18030_test.tbl_test";
        String schemaRegistryUrl = "http://kafka:8081";
        String bootstrapServers = "kafka:9092";
        String groupId = "k3";
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", bootstrapServers);
        kafkaProperties.setProperty("group.id", groupId);
        kafkaProperties.setProperty("auto.offset.reset", "earliest");
        kafkaProperties.setProperty("enable.auto.commit", String.valueOf(Boolean.valueOf(false)));

        kafkaProperties.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        kafkaProperties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        kafkaProperties.setProperty("schema.registry.url", schemaRegistryUrl);

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf(
                            "offset = %d, key=%s, value=%s \n",
                            record.offset(), record.key(), record.value().getSchema());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
