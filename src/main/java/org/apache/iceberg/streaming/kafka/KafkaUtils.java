package org.apache.iceberg.streaming.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


import java.util.*;

public class KafkaUtils {

    /**
     * 获取 Kafka Committed Offset
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
        consumer.close();

        /* 构建返回对象 */
        Map<TopicPartition, Long>  offsets = new HashMap<>();
        for(Map.Entry<TopicPartition, OffsetAndMetadata> c : committedOffsets.entrySet()){
            if(c.getValue() != null && c.getValue().offset() > endOffsets.get(c.getKey()) ){

                offsets.put(c.getKey(), c.getValue().offset());
            }else{
                offsets.put(c.getKey(), beginningOffsets.get(c.getKey()));
            }

        }
        return offsets;
    }

}
