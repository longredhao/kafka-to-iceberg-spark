package org.apache.iceberg.streaming.utils

import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.streaming.config.{RunCfg, TableCfg}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging

import java.time.Duration
import java.util
import java.util.Properties
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaSetConverter}


/**
 *  Kafka 数据处理工具类.
 */
object KafkaUtils extends Logging{

  /**
   * 获取历史的 Kafka Committed Offset
   *  - 如果 Commit Offset 则取值 beginningOffsets
   *
   * @return Kafka Committed Offset
   */
  def seekCommittedOffsets(bootstrapServers: String, groupId: String, topics: Array[String], keyDeserializer: String, valueDeserializer: String, schemaRegistryUrl: String): util.Map[TopicPartition, Long] = {
    /* 参数组装 */ val kafkaProperties = new Properties
    kafkaProperties.setProperty("bootstrap.servers", bootstrapServers)
    kafkaProperties.setProperty("group.id", groupId)
    kafkaProperties.setProperty("key.deserializer", keyDeserializer)
    kafkaProperties.setProperty("value.deserializer", valueDeserializer)
    kafkaProperties.setProperty("schema.registry.url", schemaRegistryUrl)
    kafkaProperties.setProperty("auto.offset.reset", "earliest")
    kafkaProperties.setProperty("enable.auto.commit", "false")
    /* 创建 Kafka Consumer 对象 */ val consumer = new KafkaConsumer[String, GenericRecord](kafkaProperties)
    /* 获取  committedOffsets 和 beginningOffsets */ val topicPartitions = new util.HashSet[TopicPartition]
    for (topic <- topics) {
      val partitionInfos = consumer.partitionsFor(topic).asScala

      for (partitionInfo <- partitionInfos) {
        val partition = partitionInfo.partition
        val topicPartition = new TopicPartition(topic, partition)
        topicPartitions.add(topicPartition)
      }
    }
    val committedOffsets = consumer.committed(topicPartitions)
    val beginningOffsets = consumer.beginningOffsets(topicPartitions)
    /* 构建返回对象 */
    val offsets = new util.HashMap[TopicPartition, Long]

    for (c <- committedOffsets.entrySet.asScala) {
      if (c.getValue != null && c.getValue.offset > beginningOffsets.get(c.getKey)) offsets.put(c.getKey, c.getValue.offset)
      else offsets.put(c.getKey, beginningOffsets.get(c.getKey))
    }
    consumer.close()
    offsets
  }

  def commitSync(tableCfg: TableCfg, offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    val cfg = tableCfg.getCfgAsProperties
    val bootstrapServers = cfg.getProperty(RunCfg.KAFKA_BOOTSTRAP_SERVERS)
    val groupId = cfg.getProperty(RunCfg.KAFKA_CONSUMER_GROUP_ID)
    val keyDeserializer = cfg.getProperty(RunCfg.KAFKA_CONSUMER_KEY_DESERIALIZER)
    val valueDeserializer = cfg.getProperty(RunCfg.KAFKA_CONSUMER_VALUE_DESERIALIZER)
    val schemaRegistryUrl = cfg.getProperty(RunCfg.KAFKA_SCHEMA_REGISTRY_URL)
    val kafkaCommitTimeout = cfg.getProperty(RunCfg.KAFKA_CONSUMER_COMMIT_TIMEOUT_MILLIS).toLong
    val kafkaProperties = new Properties
    kafkaProperties.setProperty("bootstrap.servers", bootstrapServers)
    kafkaProperties.setProperty("group.id", groupId)
    kafkaProperties.setProperty("key.deserializer", keyDeserializer)
    kafkaProperties.setProperty("value.deserializer", valueDeserializer)
    kafkaProperties.setProperty("schema.registry.url", schemaRegistryUrl)
    kafkaProperties.setProperty("auto.offset.reset", "earliest")
    kafkaProperties.setProperty("enable.auto.commit", "false")
    /* 创建 Kafka Consumer 对象 */
    val consumer = new KafkaConsumer[GenericRecord, GenericRecord](kafkaProperties)
    /* 提交 Offset */
    val timeout = Duration.ofMillis(kafkaCommitTimeout)
    consumer.commitSync(offsets, timeout)
    consumer.close()
  }
}
