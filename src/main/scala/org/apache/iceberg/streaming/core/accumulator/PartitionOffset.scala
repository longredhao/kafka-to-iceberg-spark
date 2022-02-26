package org.apache.iceberg.streaming.core.accumulator

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010

/**
 * Kafka Partition Offset 状态类
 * @param topic  topic name
 * @param partition partition id
 * @param fromOffset  本批次读取的 kafka 数据的起始 offset
 * @param untilOffset  本批次读取的 kafka 数据的结束 offset
 * @param curOffset 本批次当前已完成数据处理的 offset, 取值范围： fromOffset <= curOffset <= untilOffset
 */
final class PartitionOffset(
                         val topic: String,
                         val partition: Int,
                         val fromOffset: Long,
                         val untilOffset: Long,
                         var curOffset: Long) extends Serializable {
  /** Kafka TopicPartition object, for convenience */
  def topicPartition(): TopicPartition = new TopicPartition(topic, partition)

  /** 获取 Kafka Commit Offset */
  def getCommitOffsetRange: kafka010.OffsetRange =
    kafka010.OffsetRange.create(this.topic, this.partition, this.fromOffset, this.curOffset)

  /** Number of messages this OffsetRange refers to */
  def count(): Long = untilOffset - fromOffset

  /** Deep copy */
  def copy():PartitionOffset = {
    new PartitionOffset(this.topic, this.partition, this.fromOffset, this.untilOffset, this.curOffset)
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: PartitionOffset =>
      this.topic == that.topic &&
        this.partition == that.partition &&
        this.fromOffset == that.fromOffset &&
        this.untilOffset == that.untilOffset &&
        this.curOffset == that.curOffset
    case _ => false
  }

  override def hashCode(): Int = {
    (topic, partition, fromOffset, untilOffset, curOffset).hashCode()
  }

  def getAsCommittedOffset:Map[TopicPartition, Long] = {
    Map(new TopicPartition(topic,partition) -> fromOffset)
  }

  override def toString: String = {
    s"PartitionOffset(topic: '$topic', partition: $partition, range: [$fromOffset -> $untilOffset], curOffset: $curOffset)"
  }

}
