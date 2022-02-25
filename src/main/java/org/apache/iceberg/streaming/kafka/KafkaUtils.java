package org.apache.iceberg.streaming.kafka;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


import java.io.IOException;
import java.util.*;

public class KafkaUtils {

    /**
     * 获取历史的最后提交的  Kafka Committed Record 的 Schema Version, 该 Version 被用于作业初始化时的 Innit Version
     *  - 如果 Commit Offset 为空 则取值 beginningOffsets
     *  - 如果 存在 多个 topic ,则所有的 Topic 的 Schema 应当保持相同迭代版本
     * @return Kafka Committed Offset
     */
    public static int getLastCommittedSchemaVersion(
            String bootstrapServers,
            String groupId,
            String[] topics,
            String keyDeserializer,
            String valueDeserializer,
            String schemaRegistryUrl
    ) throws RestClientException, IOException {
        /* 参数组装 */
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", bootstrapServers);
        kafkaProperties.setProperty("group.id", groupId);
        kafkaProperties.setProperty("key.deserializer", keyDeserializer);
        kafkaProperties.setProperty("value.deserializer", valueDeserializer);
        kafkaProperties.setProperty("schema.registry.url", schemaRegistryUrl);
        kafkaProperties.setProperty("auto.offset.reset", "earliest");
        kafkaProperties.setProperty("enable.auto.commit", String.valueOf(Boolean.valueOf(false)));

        /* 创建 Kafka Consumer 对象 */
        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(kafkaProperties);

        /* 获取  committedOffsets 和 beginningOffsets */
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (String topic : topics){
            Collection<PartitionInfo> partitionInfos =  consumer.partitionsFor(topic);
            for(PartitionInfo partitionInfo : partitionInfos){
                int partition = partitionInfo.partition();
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                topicPartitions.add(topicPartition);
            }
        }
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(topicPartitions);
        Map<TopicPartition, Long> beginningOffsets =  consumer.beginningOffsets(topicPartitions);
        Map<TopicPartition, Long> endOffsets =  consumer.endOffsets(topicPartitions);

        SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        ArrayList<Integer> versions = new ArrayList<Integer>();

        for(Map.Entry<TopicPartition, OffsetAndMetadata> committedOffset : committedOffsets.entrySet()) {
            /* 获取 TopicPartition 和 OffsetAndMetadata */
            TopicPartition topicPartition = committedOffset.getKey();
            OffsetAndMetadata offsetAndMetadata = committedOffset.getValue();

            /* 读取 Schema 的 schemaOffset 先初始化为 beginningOffset, 如果 committedOffset 不为空且有效则更新为 committedOffset -1 */
            Long schemaOffset = beginningOffsets.get(topicPartition);
            if (offsetAndMetadata != null &&
                    offsetAndMetadata.offset() > beginningOffsets.get(topicPartition) &&
                    offsetAndMetadata.offset() < endOffsets.get(topicPartition)
            ) {
                schemaOffset = offsetAndMetadata.offset() - 1;  /* 读取上次提交的最后一条记录的 Schema */
            }

            System.out.println("committedOffset" + committedOffset.toString());

            /* 开始 seek 数据 */
            consumer.assign(Collections.singletonList(topicPartition));
//            consumer.subscribe(  Collections.singletonList(topicPartition.topic()));

            consumer.seek(topicPartition, schemaOffset);
            boolean run = true;
            while (run) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(java.time.Duration.ofSeconds(1));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    versions.add(client.getVersion(String.format("%s-value", topicPartition.topic()), record.value().getSchema()));
                    if (record.offset() == schemaOffset + 1) {
                        run = false;
                        break;
                    }
                }
            }
            consumer.unsubscribe();  /* clean */
        }
        consumer.close();
        return Collections.min(versions);
    }






    /**
     * 获取历史的 Kafka Committed Offset
     *  - 如果 Commit Offset 则取值 beginningOffsets
     * @return Kafka Committed Offset
     */
    public static java.util.Map<TopicPartition, java.lang.Long> seekCommittedOffsets(
            String bootstrapServers,
            String groupId,
            String[] topics,
            String keyDeserializer,
            String valueDeserializer,
            String schemaRegistryUrl
    ) {
        /* 参数组装 */
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", bootstrapServers);
        kafkaProperties.setProperty("group.id", groupId);
        kafkaProperties.setProperty("key.deserializer", keyDeserializer);
        kafkaProperties.setProperty("value.deserializer", valueDeserializer);
        kafkaProperties.setProperty("schema.registry.url", schemaRegistryUrl);
        kafkaProperties.setProperty("auto.offset.reset", "earliest");
        kafkaProperties.setProperty("enable.auto.commit", String.valueOf(Boolean.valueOf(false)));

        /* 创建 Kafka Consumer 对象 */
        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(kafkaProperties);

        /* 获取  committedOffsets 和 beginningOffsets */
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (String topic : topics){
            Collection<PartitionInfo> partitionInfos =  consumer.partitionsFor(topic);
            for(PartitionInfo partitionInfo : partitionInfos){
                int partition = partitionInfo.partition();
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                topicPartitions.add(topicPartition);
            }
        }
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(topicPartitions);
        Map<TopicPartition, Long> beginningOffsets =  consumer.beginningOffsets(topicPartitions);
        Map<TopicPartition, Long> endOffsets =  consumer.endOffsets(topicPartitions);


        /* 构建返回对象 */
        Map<TopicPartition, Long>  offsets = new HashMap<>();
        for(Map.Entry<TopicPartition, OffsetAndMetadata> c : committedOffsets.entrySet()){
            if(c.getValue() != null && c.getValue().offset() > beginningOffsets.get(c.getKey()) ){
                offsets.put(c.getKey(), c.getValue().offset());
            }else{
                offsets.put(c.getKey(), beginningOffsets.get(c.getKey()));
            }

        }
        return offsets;
    }

}
