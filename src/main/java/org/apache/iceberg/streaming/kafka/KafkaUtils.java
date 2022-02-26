package org.apache.iceberg.streaming.kafka;


import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.streaming.config.RunCfg;
import org.apache.iceberg.streaming.config.TableCfg;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class KafkaUtils {

    public static void commitAsync(TableCfg tableCfg, Map<TopicPartition, OffsetAndMetadata> offsets) throws IOException {
        Properties cfg = tableCfg.getCfgAsProperties();
        String bootstrapServers = cfg.getProperty(RunCfg.KAFKA_BOOTSTRAP_SERVERS);
        String groupId = cfg.getProperty(RunCfg.KAFKA_CONSUMER_GROUP_ID);
        String[] topics = cfg.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC).split(",");
        String keyDeserializer = cfg.getProperty(RunCfg.KAFKA_CONSUMER_KEY_DESERIALIZER);
        String valueDeserializer =  cfg.getProperty(RunCfg.KAFKA_CONSUMER_VALUE_DESERIALIZER);
        String schemaRegistryUrl =  cfg.getProperty(RunCfg.KAFKA_SCHEMA_REGISTRY_URL);
        String kafkaCommitTimeout = cfg.getProperty(RunCfg.KAFKA_CONSUMER_COMMIT_TIMEOUT_MILLIS);

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

        /* 提交 Offset */
        Duration timeout =  Duration.ofMillis(Long.parseLong(kafkaCommitTimeout));
        consumer.commitSync(offsets, timeout);
        consumer.close();
    }



    /**
     *   获取 Schema Version, 该 Version 被用于作业初始化时的 Innit Version
     *  - 如果 Commit Offset 为空 则取值 beginningOffsets
     *  - 如果 存在 多个 topic ,则所有的 Topic 的 Schema 应当保持相同迭代版本
     * @return Kafka Committed Offset
     */
    public static int getCurrentSchemaVersion(TableCfg tableCfg) throws RestClientException, IOException {
        Properties cfg = tableCfg.getCfgAsProperties();
        String bootstrapServers = cfg.getProperty(RunCfg.KAFKA_BOOTSTRAP_SERVERS);
        String groupId = cfg.getProperty(RunCfg.KAFKA_CONSUMER_GROUP_ID);
        String[] topics = cfg.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC).split(",");
        String keyDeserializer = cfg.getProperty(RunCfg.KAFKA_CONSUMER_KEY_DESERIALIZER);
        String valueDeserializer =  cfg.getProperty(RunCfg.KAFKA_CONSUMER_VALUE_DESERIALIZER);
        String schemaRegistryUrl =  cfg.getProperty(RunCfg.KAFKA_SCHEMA_REGISTRY_URL);
       return getCurrentSchemaVersion(
               bootstrapServers,
               groupId,
               topics,
               keyDeserializer,
               valueDeserializer,
               schemaRegistryUrl
       );
    }
        /**
         *   获取 Schema Version, 该 Version 被用于作业初始化时的 Innit Version
         *  - 如果 Commit Offset 为空 则取值 beginningOffsets
         *  - 如果 存在 多个 topic ,则所有的 Topic 的 Schema 应当保持相同迭代版本
         * @return Kafka Committed Offset
         */
    public static int getCurrentSchemaVersion(
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

        /* 判断 committedOffsets 是否等于 endOffsets  */
        /* 如果 committedOffsets == endOffsets, 则当前所有的数据均已处理完毕, fromOffset = endOffset - 1, 即取最后一条记录的 Schema */
        /* 如果 committedOffsets < endOffsets, 则当前所有的数据均已处理完毕, fromOffset = endOffset - 1, 即取最后一条记录的 Schema */

        SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        ArrayList<Integer> versions = new ArrayList<>();

        for(Map.Entry<TopicPartition, OffsetAndMetadata> committedOffset : committedOffsets.entrySet()) {
            /* 获取 TopicPartition 和 OffsetAndMetadata */
            TopicPartition topicPartition = committedOffset.getKey();
            OffsetAndMetadata committedMetadata = committedOffset.getValue();

            /* 读取 Schema 的 fromOffset 先初始化为 beginningOffset, 如果 committedOffset 不为空且有效则更新为 committedOffset */
            Long fromOffset = beginningOffsets.get(topicPartition);
            if (committedMetadata != null &&
                    committedMetadata.offset() > beginningOffsets.get(topicPartition) &&
                    committedMetadata.offset() < endOffsets.get(topicPartition)) {
                fromOffset = committedMetadata.offset();  /* 读取 committed offset 的后一条记录的 Schema */
            }else if(committedMetadata != null &&
                    committedMetadata.offset() > beginningOffsets.get(topicPartition) &&
                    committedMetadata.offset() == endOffsets.get(topicPartition)){
                fromOffset = committedMetadata.offset() - 1;  /* 读取 committed offset 的所在记录的 Schema */

            }

            /* 开始 seek 数据 */
            consumer.assign(Collections.singletonList(topicPartition));

            /* 如果 1 * 3 秒内没有读取到任何数据, 则认定为该 Kafka Partition 队列为空, 结束读取等待. */
            consumer.seek(topicPartition, fromOffset);
            int loopTimes = 3;
            boolean loopFlag = true;
            while (loopFlag && loopTimes-- >0){
                ConsumerRecords<String, GenericRecord> records = consumer.poll(java.time.Duration.ofSeconds(1));
                if(!records.isEmpty()) {
                    ConsumerRecord<String, GenericRecord> record = records.iterator().next();
                    versions.add(client.getVersion(String.format("%s-value", topicPartition.topic()),
                            new AvroSchema(record.value().getSchema())));
                    loopFlag = false;
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

        /* 构建返回对象 */
        Map<TopicPartition, Long>  offsets = new HashMap<>();
        for(Map.Entry<TopicPartition, OffsetAndMetadata> c : committedOffsets.entrySet()){
            if(c.getValue() != null && c.getValue().offset() > beginningOffsets.get(c.getKey()) ){
                offsets.put(c.getKey(), c.getValue().offset());
            }else{
                offsets.put(c.getKey(), beginningOffsets.get(c.getKey()));
            }

        }
        consumer.close();
        return offsets;
    }

}
