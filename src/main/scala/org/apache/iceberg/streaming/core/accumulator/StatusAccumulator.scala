package org.apache.iceberg.streaming.core.accumulator

import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.streaming.config.{RunCfg, TableCfg}
import org.apache.iceberg.streaming.utils.KafkaUtils
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.util.AccumulatorV2

import java.io.StringReader
import java.util
import java.util.Properties
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.immutable.HashMap

/**
 * 共享状态累加器.
 */
final class StatusAccumulator extends AccumulatorV2[HashMap[String, PartitionOffset], HashMap[String, PartitionOffset]] {

  /* Kafka Partitions Record Offset , Key Format: topic:partition */
  private var _partitionOffsets :HashMap[String, PartitionOffset] = _

  /* Schema Register Server's  Schema Version*/
  private var _schemaVersion :Int = 0

  def partitionOffsets :HashMap[String, PartitionOffset] = _partitionOffsets
  def schemaVersion :Int = _schemaVersion

  override def isZero: Boolean = true

  /* Reset curOffset back to fromOffset */
  override def reset(): Unit = {
    for(v <- _partitionOffsets.values){
      v.curOffset = v.fromOffset
    }
  }
  override def add(v: HashMap[String, PartitionOffset]): Unit = _partitionOffsets ++= v
  override def value: HashMap[String, PartitionOffset] = null

  override def copy(): StatusAccumulator = {
    val newAcc = new StatusAccumulator
    var copyValue :HashMap[String, PartitionOffset] = new HashMap[String, PartitionOffset]()
    for(v <- _partitionOffsets){
      copyValue += (v._1 -> v._2.copy())
    }
    newAcc._partitionOffsets = copyValue
    newAcc._schemaVersion = _schemaVersion
    newAcc
  }

  /** 合并 StatusAccumulator */
  override def merge(other: AccumulatorV2[HashMap[String, PartitionOffset], HashMap[String, PartitionOffset]]): Unit = {
    other match {
      case o: StatusAccumulator =>
        for(partition <- _partitionOffsets){
          partition._2.curOffset = Math.max(partition._2.curOffset, o._partitionOffsets(partition._1).curOffset)
        }
        _schemaVersion = Math.min(_schemaVersion, o._schemaVersion)
      case _ =>
        throw new UnsupportedOperationException(s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")

    }
  }

  override def hashCode(): Int = {
    val state = Seq(partitionOffsets, schemaVersion)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def updatePartitionOffsets(recordOffsets: HashMap[String, PartitionOffset]): StatusAccumulator = {
    _partitionOffsets = recordOffsets
    this
  }

  /**
   * 程序第一次启动或重启时,从 Kafka 中读取 committedOffsets 并作为消费的开始位点.
   * @param committedOffsets 从 Kafka 中读取的 committedOffsets 值
   * @return StatusAccumulator
   */
  def initPartitionOffset(committedOffsets: Map[TopicPartition, Long]): StatusAccumulator = {
    var partitionOffsets = new HashMap[String, PartitionOffset]()

    val iterator = committedOffsets.iterator
    while (iterator.hasNext){
      val committedOffset =  iterator.next()
      val topicPartition = committedOffset._1
      val topic = topicPartition.topic()
      val partition = topicPartition.partition()
      val offset = committedOffset._2
      partitionOffsets += (s"$topic:$partition" -> new PartitionOffset(topic, partition,offset, 0, offset))
    }
    _partitionOffsets = partitionOffsets
    this
  }

  /**
   * 程序第一次启动或重启时,从 Kafka 中读取 committedOffsets 并作为消费的开始位点.
   * @param tableCfg TableCfg
   * @return StatusAccumulator
   */
  def initPartitionOffset(tableCfg: TableCfg): StatusAccumulator = {
    val cfg: Properties = new Properties
    cfg.load(new StringReader(tableCfg.getConfValue))
    val bootstrapServers: String = cfg.getProperty(RunCfg.KAFKA_BOOTSTRAP_SERVERS)
    val groupId: String = cfg.getProperty(RunCfg.KAFKA_CONSUMER_GROUP_ID)
    val topics: Array[String] = cfg.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC).split(",")
    val keyDeserializer: String = cfg.getProperty(RunCfg.KAFKA_CONSUMER_KEY_DESERIALIZER)
    val valueDeserializer: String = cfg.getProperty(RunCfg.KAFKA_CONSUMER_VALUE_DESERIALIZER)
    val schemaRegistryUrl: String = cfg.getProperty(RunCfg.KAFKA_SCHEMA_REGISTRY_URL)
    val committedOffsets =  KafkaUtils.seekCommittedOffsets(bootstrapServers, groupId, topics,
      keyDeserializer, valueDeserializer, schemaRegistryUrl)
    initPartitionOffset(committedOffsets.asScala.toMap)
  }


  def setSchemaVersion(schemaVersion: Int): StatusAccumulator = {
    _schemaVersion = schemaVersion
    this
  }

  /**
   * 更新当前完成解析数据解析处理的 Offset :
   * @param record  ConsumerRecord[String, GenericRecord]
   */
  def updateCurOffset(record: ConsumerRecord[GenericRecord, GenericRecord]): Unit ={

    _partitionOffsets(s"${record.topic()}:${record.partition()}").curOffset = record.offset() + 1
  }

  def getCurrentOffsetRangers: Array[OffsetRange] = {
    _partitionOffsets.values.map(x => OffsetRange.apply(x.topic, x.partition, x.fromOffset, x.curOffset)).toArray
  }

  /**
   * 将 _partitionOffsets 转换为可用于 Kafka Consumer 提交的 Offsets
   * @return
   */
  def convertToKafkaCommitOffset(): util.Map[TopicPartition, OffsetAndMetadata] = {
    val offsets = new java.util.HashMap[TopicPartition, OffsetAndMetadata]()
    for(po <- _partitionOffsets.values){
      offsets.put(new TopicPartition(po.topic, po.partition), new OffsetAndMetadata(po.curOffset))
    }
    offsets
  }

//  /**
//   * 判断是否所有的 Partition 的 Schema 都发生了更新
//   * 如果 当前被处理消息的 curOffset < untilOffset 则该 Partition 的 Schema 发生了更新
//   * @return
//   */
//  def isAllPartSchemaChanged(tableCfg: TableCfg): Boolean = {
//     /*  如果 curOffset < untilOffset,  则认为 Schema 发生了变动, 如果所有 offset 大于0的数据分区均发生了更新,则认为所有的分区都发生了更新 */
//    val po =  _partitionOffsets.values
//    /* 如果所有分区的 curOffset < untilOffset, 即所有的分区均丢弃了部分数据, 即认定为所有的 partition 的 schema 都发生了更新 */
//    if(po.size == po.count(p => p.curOffset < p.untilOffset)){
//      return true
//    }
//    /* 否则根据 curOffset, 从 Kafka 中读取所有分区的后一条数据( curOffset + 1), 并获取所有分区数据的最小 schema version, 然后与当前 _schemaVersion 对比 */
//    val nextBatchMinSchemaVersion  = KafkaUtils.getNextBatchMinSchemaVersion(tableCfg)
//    if(nextBatchMinSchemaVersion > _schemaVersion){
//       true
//    }else{
//       false
//    }
//  }



  def upgradeSchemaVersion(step: Int): Unit = {
    _schemaVersion += step
  }

  override def toString: String =
    s"""
      |        StatusAccumulator(
      |            partitionOffsets: Map(
      |                ${_partitionOffsets.mkString(",\n                ")}
      |                ),
      |            schemaVersion: ${_schemaVersion}
      |        )
      |""".stripMargin
}


object StatusAccumulator {
  def registerInstance(sc: SparkContext, name: String): StatusAccumulator = {
    val statusAccumulator = new StatusAccumulator
    sc.register(statusAccumulator, name)
    statusAccumulator
  }

}
