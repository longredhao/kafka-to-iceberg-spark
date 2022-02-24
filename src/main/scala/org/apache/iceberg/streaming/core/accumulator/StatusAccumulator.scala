package org.apache.iceberg.streaming.core.accumulator

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.util.AccumulatorV2

import scala.collection.immutable.HashMap

final class StatusAccumulator extends AccumulatorV2[HashMap[String, PartitionOffset], HashMap[String, PartitionOffset]] {

  /* Kafka Partitions Record Offset */
  private var _partitionOffsets :HashMap[String, PartitionOffset] = _

  /* Schema Register Server's  Schema Version*/
  private var _schemaVersion :Int = 0

  def recordOffsets :HashMap[String, PartitionOffset] = _partitionOffsets
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
    val state = Seq(recordOffsets, schemaVersion)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def setPartitionOffsets(recordOffsets: HashMap[String, PartitionOffset]): StatusAccumulator = {
    _partitionOffsets = recordOffsets
    this
  }
  def setSchemaVersion(schemaVersion: Int): StatusAccumulator = {
    _schemaVersion = schemaVersion
    this
  }

  /**
   * 更新当前完成解析数据解析处理的 Offset :
   * @param record  ConsumerRecord[String, GenericRecord]
   */
  def updateCurOffset(record: ConsumerRecord[String, GenericRecord]): Unit ={
    _partitionOffsets(s"${record.topic()}:${record.partition()}").curOffset = record.offset() + 1
  }

  def getCommitOffsetRangers: Array[OffsetRange] = {
    _partitionOffsets.values.map(x => OffsetRange.apply(x.topic, x.partition, x.fromOffset, x.curOffset)).toArray
  }

  /**
   * 判断是否所有的 Partition 的 Schema 都发生了更新
   * 如果 当前被处理消息的 curOffset < untilOffset 则该 Partition 的 Schema 发生了更新
   * @return
   */
  def isAllPartSchemaChanged: Boolean = {
    for(partitionOffset <-  _partitionOffsets.values){
      if(partitionOffset.curOffset == partitionOffset.untilOffset) {
        return false
      }
    }
    true
  }

  def upgradeSchemaVersion(): Unit = {
    _schemaVersion += 1
  }
}


object StatusAccumulator {
  def registerInstance(sc: SparkContext, name: String): StatusAccumulator = {
    val statusAccumulator = new StatusAccumulator
    sc.register(statusAccumulator, name)
    statusAccumulator
  }
}
