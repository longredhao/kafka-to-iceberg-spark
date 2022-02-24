package org.apache.iceberg.streaming.write

import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.streaming.Kafka2Iceberg.{schemaBroadcastMaps, statusAccumulatorMaps}
import org.apache.iceberg.streaming.avro.schema.SchemaUtils
import org.apache.iceberg.streaming.avro.{AvroConversionHelper, TimestampZoned, TimestampZonedFactory}
import org.apache.iceberg.streaming.avro.schema.broadcast.SchemaBroadcast
import org.apache.iceberg.streaming.config.{RunCfg, TableCfg}
import org.apache.iceberg.streaming.core.accumulator
import org.apache.iceberg.streaming.core.accumulator.StatusAccumulator
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.util.Properties
import scala.collection.immutable.HashMap


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
  def write2(spark: SparkSession,
            rdd: RDD[ConsumerRecord[String, GenericRecord]],
            tableCfg: TableCfg,
            useCfg: Properties
           ): Unit = {

    do{

    }while(1>2)

    /* 解析配置信息. */
    val cfg = tableCfg.getCfgAsProperties
    val icebergTableName: String = cfg.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val statusAcc: StatusAccumulator = statusAccumulatorMaps(icebergTableName)
    val schemaBroadcast: Broadcast[SchemaBroadcast] = schemaBroadcastMaps(icebergTableName)

    /* 解析 Schema Broadcast 以及 Accumulator 变量值 */
    val hisAvroSchemaVersion = statusAcc.schemaVersion
    val hisSchemaToVersionMap = schemaBroadcast.value.schemaToVersionMap
    val hisVersionToSchemaMap = schemaBroadcast.value.versionToSchemaMap
    val hisMergedSchemaStr = schemaBroadcast.value.mergedSchemaStr
    val hisMergedStruct = schemaBroadcast.value.mergedStruct

    /* 通过 History Avro Schema Version 获取历史的 Avro Schema 信息 */
    val hisAvroSchemaStr = hisVersionToSchemaMap(hisAvroSchemaVersion)
    val hisAvroSchema = new Schema.Parser().parse(hisAvroSchemaStr)
    val hisStructType = SchemaUtils.convertSchemaToStructType(hisAvroSchema)

    val rddRow = rdd.mapPartitions(
      records => {
        /* Spark 3.2 Dependency on Avro Version 1.8.2, But Avro Schema not Serializable Before Version 1.9.0 ,So Use String Type instead . */
        /* Schema hashCode Value is Different in Each Executors, So Need Recompute Schema in Each Executor. */
        /* hisAvroSchemaHash 用于快速对比判断当前处理记录的 Schema hashCode 是否更新，不考虑 hash 碰撞问题 */
        val hisAvroSchema = new Schema.Parser().parse(hisAvroSchemaStr)
        val hisAvroSchemaHash = hisAvroSchema.hashCode()
        val convertor = AvroConversionHelper.createConverterToRow(hisAvroSchema, hisStructType)

        val sourceIndex: Seq[Int] = SchemaUtils.getSourceFieldIndex(tableCfg, hisAvroSchema)
        val transactionIndex: Seq[Int] =  SchemaUtils.getTransactionIndex(tableCfg, hisAvroSchema)
        val kafkaColumns: Seq[String] = tableCfg.getCfgAsProperties.getProperty(RunCfg.RECORD_METADATA_KAFKA_COLUMNS).split(",").map(_.trim)

        records.map {
          record: ConsumerRecord[String, GenericRecord] => {
            if (record.value.getSchema.hashCode().equals(hisAvroSchemaHash)) {
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
    /* 如果 Schema 未发生改变. */
    if (statusAcc.isAllPartSchemaChanged) {
      /* 更新 Schema Version */
      statusAcc.upgradeSchemaVersion
      /* 更新 SchemaBroadcaster 信息. */
      val schemaToVersionMap: HashMap[String, Integer] = SchemaUtils.getSchemaToVersionMap(tableCfg)
      val versionToSchemaMap: HashMap[Integer, String] = SchemaUtils.getVersionToSchemaMap(tableCfg)
      val mergedSchemaStr: String = SchemaUtils.getInitMergedSchemaStr(tableCfg)
      val mergedStruct: StructType = SchemaUtils.convertSchemaToStructType(mergedSchemaStr)
      val schemaBroadcast = SchemaBroadcast(schemaToVersionMap, versionToSchemaMap, mergedSchemaStr, mergedStruct)
      schemaBroadcastMaps += (icebergTableName -> spark.sparkContext.broadcast(schemaBroadcast))
    }
  }



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
//    def write(spark: SparkSession,
//              rdd: RDD[ConsumerRecord[String, GenericRecord]],
//              tableCfg: TableCfg,
//              useCfg: Properties
//             ): Unit = {
//      /* 解析配置信息. */
//      val cfg = tableCfg.getCfgAsProperties
//      val icebergTableName: String = cfg.getProperty(RunCfg.ICEBERG_TABLE_NAME)
//      val schemaAcc: SchemaAccumulatorV1 = statusAccumulatorMaps(icebergTableName)
//      val schemaBroadcast: Broadcast[SchemaBroadcast] = schemaBroadcastMaps(icebergTableName)
//
//      /* 解析 Schema Broadcast 以及 Accumulator 变量值 */
//      val hisAvroSchemaVersion = schemaAcc.value._version
//      val hisSchemaToVersionMap = schemaBroadcast.value.schemaToVersionMap
//      val hisVersionToSchemaMap = schemaBroadcast.value.versionToSchemaMap
//      val hisMergedSchemaStr = schemaBroadcast.value.mergedSchemaStr
//      val hisMergedStruct = schemaBroadcast.value.mergedStruct
//
//      /* 通过 History Avro Schema Version 获取历史的 Avro Schema 信息 */
//      val hisAvroSchemaStr = hisVersionToSchemaMap(hisAvroSchemaVersion)
//      val hisAvroSchema = new Schema.Parser().parse(hisAvroSchemaStr)
//      val hisStructType = SchemaUtils.convertSchemaToStructType(hisAvroSchema)
//
//      /* 对比(diff) hisMergedStruct 和 hisStructType 计算被删除的列信息 */
//      val droppedStructField: Array[StructField] = hisMergedStruct.fields diff hisStructType.fields
//
//      val droppedFieldSize = droppedStructField.length /* 历史所有被删除的列数. */
//
//
//      val rddRow = rdd.mapPartitions(
//        records => {
//          /* Spark 3.2 Dependency on Avro Version 1.8.2, But Avro Schema not Serializable Before Version 1.9.0 ,So Use String Type instead . */
//          /* Schema hashCode Value is Different in Each Executors, So Need Recompute Schema in Each Executor. */
//          /* hisAvroSchemaHash 用于快速对比判断当前处理记录的 Schema hashCode 是否更新，不考虑 hash 碰撞问题 */
//          val hisAvroSchema = new Schema.Parser().parse(hisAvroSchemaStr)
//          val hisAvroSchemaHash = hisAvroSchema.hashCode()
//          val convertor = AvroConversionHelper.createConverterToRow(hisAvroSchema, hisStructType)
//
//          //        val sourceIndex: Seq[Integer] =
//          //        val transactionIndex: Seq[Integer] =
//          //        val beforeIndex: Seq[Integer] =
//          //        val afterIndex: Seq[Integer] =
//          //        val kafkaColumns: Seq[String] =
//
//          records.map {
//            x => {
//
//              if (x.value.getSchema.hashCode().equals(hisAvroSchemaHash)) {
//
//
//              }
//              else {
//                val curAvroSchemaStr = x.value().getSchema.toString
//                /* 如果当前 schemaVersionMap 中没有 当前的Avro 版本信息，则重新从 Schema Registry Server 加载 Avro Schema 信息. */
//                if (!hisSchemaToVersionMap.contains(curAvroSchemaStr)) {
//                  val schemaToVersionMap = SchemaUtils.getSchemaToVersionMap(tableCfg)
//                  val versionToSchemaMap = SchemaUtils.getVersionToSchemaMap(tableCfg)
//
//                  val mergedSchemaStr: String = SchemaUtils.getUpdateMergedSchemaStr(hisMergedSchemaStr, curAvroSchemaStr)
//                  val mergedStruct: StructType = SchemaUtils.convertSchemaToStructType(mergedSchemaStr)
//
//                  val newSchemaBroadcastValue = SchemaBroadcast(schemaToVersionMap, versionToSchemaMap, mergedSchemaStr, mergedStruct)
//                  val newSchemaBroadcast = spark.sparkContext.broadcast(newSchemaBroadcastValue)
//                  schemaBroadcast.unpersist(false)
//                  schemaBroadcast.destroy()
//                  schemaBroadcastMaps += (icebergTableName -> newSchemaBroadcast)
//                  schemaAcc.updateVersion(schemaToVersionMap(icebergTableName))
//                } else {
//                  schemaAcc.updateVersion(hisSchemaToVersionMap(icebergTableName))
//                }
//                Row.empty
//              }
//            }
//          }
//        }
//      )
//      rddRow.cache()
//      val counts = rddRow.count()
//      logInfo(s"Generate Rdd[Row], Record Count [$counts]")
//
//      logInfo(s"Generate Rdd[Row], RDD collect [${rddRow.collect().mkString("Array(", ", ", ")")}]")
//      /* 如果 Schema 未发生改变. */
//      if (!schemaAcc.value._existChanged) {
//
//        /* 清理 Cache. */
//        rddRow.unpersist()
//        /* 如果 Schema 发生改变. */
//      } else {
//        /* 清理 Cache. */
//        rddRow.unpersist()
//
//      }
//    }
//
//



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
//        val index = transaction.asInstanceOf[Row].fieldIndex("io.confluent.connect.avro.ConnectDefault")
//        val fieldRow =  transactionRow.getAs(index).asInstanceOf[Row]
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
