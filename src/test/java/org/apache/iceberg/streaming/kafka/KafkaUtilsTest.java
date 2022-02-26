package org.apache.iceberg.streaming.kafka;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;

public class KafkaUtilsTest {

    @Test
    public void seekLastOffsets() throws IOException {

        String topic = "test.db_gb18030_test.tbl_test,test.transaction";
        String schemaRegistryUrl = "http://kafka:8081";
        String keyDeserializer = KafkaAvroDeserializer.class.getName();
        String valueDeserializer =  KafkaAvroDeserializer.class.getName();
        String bootstrapServers = "kafka:9092";
        String groupId = "g1-1";
        java.util.Map<TopicPartition, java.lang.Long> offsets =
                KafkaUtils.seekCommittedOffsets(bootstrapServers, groupId, topic.split(","),
                        keyDeserializer, valueDeserializer, schemaRegistryUrl);
        System.out.println(offsets);
    }

    @Test
    public void getCurrentSchemaVersion() throws RestClientException, IOException {
        String topic = "test.db_gb18030_test.tbl_test";
        String schemaRegistryUrl = "http://kafka:8081";
        String keyDeserializer = KafkaAvroDeserializer.class.getName();
        String valueDeserializer =  KafkaAvroDeserializer.class.getName();
        String bootstrapServers = "kafka:9092";
        String groupId = "c1";
        int lastSchemaVersion =
                KafkaUtils.getCurrentSchemaVersion(bootstrapServers, groupId, topic.split(","),
                        keyDeserializer, valueDeserializer, schemaRegistryUrl);
        System.out.println(lastSchemaVersion);


        String groupId2 = "g1-3";
        int lastSchemaVersion2 =
                KafkaUtils.getCurrentSchemaVersion(bootstrapServers, groupId2, topic.split(","),
                        keyDeserializer, valueDeserializer, schemaRegistryUrl);
        System.out.println(lastSchemaVersion2);


    }
}