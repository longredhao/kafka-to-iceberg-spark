package org.apache.iceberg.streaming.write

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.streaming.Kafka2Iceberg.{schemaBroadcastMaps, statusAccumulatorMaps}
import org.apache.iceberg.streaming.avro.schema.SchemaUtils
import org.apache.iceberg.streaming.avro.AvroConversionHelper
import org.apache.iceberg.streaming.avro.schema.broadcast.SchemaBroadcast
import org.apache.iceberg.streaming.config.{RunCfg, TableCfg}
import org.apache.iceberg.streaming.core.accumulator.StatusAccumulator
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Row, SparkSession}

import java.util.Properties


object IcebergWriter  extends Logging {


  /**
   * Schema 版本变化词缀定义：
   *   - hisXXSchema 上次微批处理结果后的历史变量，即当前微批处理的初始输入状态
   *   - curXXSchema 当前被数据数据的内容变量
   *   - newXX:  当 hisXX 和 curXX 状态不相等时候, 合并 hisXX 和 curXX 并记为 newXX
   *
   * @param spark    SparkSession
   * @param rdd      读取的 Kafka 数据
   * @param tableCfg 数据处理配置信息
   * @param useCfg   用户通过 main 函数输入的配置信息
   */
  def write(spark: SparkSession,
            rdd: RDD[ConsumerRecord[String, GenericRecord]],
            tableCfg: TableCfg,
            useCfg: Properties
           ): Unit = {
    /* 解析配置信息. */
    val cfg = tableCfg.getCfgAsProperties
    val icebergTableName: String = cfg.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val statusAcc: StatusAccumulator = statusAccumulatorMaps(icebergTableName)
    val schemaBroadcast: Broadcast[SchemaBroadcast] = schemaBroadcastMaps(icebergTableName)

    val rddRow = rdd.mapPartitions(
      records => {
        /* Spark 3.2 Dependency on Avro Version 1.8.2, But Avro Schema not Serializable Before Version 1.9.0 ,So Use String Type instead . */
        /* Schema hashCode Value is Different in Each Executors, So Need Recompute Schema in Each Executor. */
        /* hisAvroSchemaHash 用于快速对比判断当前处理记录的 Schema hashCode 是否更新，不考虑 hash 碰撞问题 */
        val curSchemaVersion = statusAcc.schemaVersion
        val curSchemaStr =  schemaBroadcast.value.versionToSchemaMap(curSchemaVersion)
        val curSchema = new Schema.Parser().parse(curSchemaStr)
        val curSchemaHashCode = curSchema.hashCode()
        val curStructType = SchemaUtils.convertSchemaToStructType(curSchema)

        val convertor = AvroConversionHelper.createConverterToRow(curSchema, curStructType)
        val sourceIndex: Seq[Int] = SchemaUtils.getSourceFieldIndex(tableCfg, curSchema)
        val transactionIndex: Seq[Int] =  SchemaUtils.getTransactionIndex(tableCfg, curSchema)
        val kafkaColumns: Seq[String] = tableCfg.getCfgAsProperties.getProperty(RunCfg.RECORD_METADATA_KAFKA_COLUMNS).split(",").map(_.trim)

        records.map {
          record: ConsumerRecord[String, GenericRecord] => {
            if (record.value.getSchema.hashCode().equals(curSchemaHashCode)) {
              statusAcc.updateCurOffset(record)
              convertorGenericRecordToRow(record, convertor, sourceIndex, transactionIndex,kafkaColumns)
            }
            else {
              Row.empty
            }
          }
        }
      }
    )

    logInfo(s"Generate Rdd[Row]")

    logInfo(s"Generate Rdd[Row], RDD collect [${rddRow.collect().mkString("Array(", ", ", ")")}]")

  }



  /**
   * 将 GenericRecord 对象转换为 Spark SQL Row 对象 并 扁平化 Row
   *
   * @param record    GenericRecord, 从中截取附加信息, 比如消息在 kafka 中的 topic， partition,offset,timestamp 等
   * @param convertor 将 GenericRecord 对象转换为 Spark SQL Row 对象
   * @return Row 扁平化处理的结构, Row 结构 [source, opType, debeziumTime, before/after ]
   */
  def convertorGenericRecordToRow(
                                   consumerRecord: ConsumerRecord[String, GenericRecord],
                                   convertor: AnyRef => AnyRef,
                                   sourceIndex: Seq[Int],
                                   transactionIndex: Seq[Int],
                                   kafkaColumns: Seq[String]
                                 ): Row = {
    val record = consumerRecord.value()
    if (record == null) {
      return null
    }
    /* 将 GenericRecord 对象转换为 Spark SQL Row 对象 */
    val row = convertor(record).asInstanceOf[Row]
    val recordSchema = record.getSchema

    /* Unwrap Spark SQL Row 对象: 扁平化处理,并附加 Metadata 信息 */
    val opType: String = record.get("op").toString
    val dataRow: Row = {
      if (opType.equals("u") || opType.equals("i") || opType.equals("r")) {
        row.get(recordSchema.getField("after").pos()).asInstanceOf[Row]
      }
      else if (opType.equals("d")) {
        row.get(recordSchema.getField("before").pos()).asInstanceOf[Row]
      }
      else {
        null
      }
    }
    if (dataRow != null) {
      val sourceSize = sourceIndex.size
      val transactionSize = transactionIndex.size

      val valueSize = sourceSize + transactionSize + 2 + kafkaColumns.size + dataRow.size
      val values = new Array[Any](valueSize)

      /* 附加 Metadata Source 信息 */
      val sourceRow: Row = row.get(recordSchema.getField("source").pos()).asInstanceOf[Row]
      for (i <- 0 until sourceSize) {
        values.update(i, sourceRow(sourceIndex(i)))
      }

      /* 附加 Metadata Transaction 信息 */
      val transaction = row.get(record.getSchema.getField("transaction").pos())
      if(transaction != null){
        val transactionRow: Row = transaction.asInstanceOf[Row]
        for (i <- 0 until transactionSize) {
          values.update(sourceSize + i, transactionRow(transactionIndex(i)))
        }
      }else{
        for (i <- 0 until transactionSize) {
          values.update(sourceSize + i, null)
        }
      }

      /* 附加 Metadata  opType / debeziumTime */
      val debeziumTime: Long = row.get(record.getSchema.getField("ts_ms").pos()).asInstanceOf[Long]
      values.update(sourceSize + transactionSize, opType)
      values.update(sourceSize + transactionSize + 1, debeziumTime)

      /* 附加 Kafka 相关信息: topic, partition, offset, timestamp*/
      val kafkaIndexOffset = sourceSize + transactionSize + 2
      for (i <- kafkaColumns.indices) {
        kafkaColumns(i) match {
          case "topic" => values.update(kafkaIndexOffset + i, consumerRecord.topic())
          case "partition" => values.update(kafkaIndexOffset + i, consumerRecord.partition())
          case "offset" => values.update(kafkaIndexOffset + i, consumerRecord.offset())
          case "timestamp" => values.update(kafkaIndexOffset + i, consumerRecord.timestamp())
          case _ => logWarning(s"Unknown kafka metadata [${kafkaColumns(i)}]")
        }
      }

      /* 附加 Column Data Value */
      val dataIndexOffset = kafkaIndexOffset + kafkaColumns.size
      for (i <- 0 until dataRow.size) {
        values.update(dataIndexOffset + i, dataRow(i))
      }
      /* 返回 GenericRow 对象 */
      new GenericRow(values)
    }else{
      Row.empty
    }
  }

}
