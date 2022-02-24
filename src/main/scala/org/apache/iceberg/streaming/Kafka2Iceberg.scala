package org.apache.iceberg.streaming


import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.streaming.avro.schema.SchemaUtils
import org.apache.iceberg.streaming.avro.schema.broadcast.SchemaBroadcast
import org.apache.iceberg.streaming.avro.{TimestampZoned, TimestampZonedFactory}
import org.apache.iceberg.streaming.config.{Config, JobCfgHelper, RunCfg}
import org.apache.iceberg.streaming.write.IcebergWriter
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.iceberg.streaming.core.accumulator.{PartitionOffset, StatusAccumulator}
import org.apache.iceberg.streaming.core.accumulator

import java.io.StringReader
import java.util.Properties
import scala.collection.immutable.HashMap



class Kafka2Iceberg {
}

object Kafka2Iceberg extends Logging{

  /* Schema 共享数据信息 （Broadcast / Accumulator). */
  var statusAccumulatorMaps: HashMap[String, StatusAccumulator] = _
  var schemaBroadcastMaps: HashMap[String, Broadcast[SchemaBroadcast]] = _

  /**
   * 主函数入口.
   * @param args Array[String]
   */
  def main(args: Array[String]): Unit = {
    /* User Input Config */
    val useCfg = Config.getInstance().loadConf(args).toProperties
    val confKey: String = useCfg.getProperty("confKey")
    val batchDuration = useCfg.getProperty("batchDuration").toInt
    val master =  useCfg.getProperty("master", "yarn")
    val runEnv = useCfg.getProperty("runEnv", RunCfg.RUN_ENV_DEFAULT) /* test or product. */

    /* Load Job Config Form Config Database */
    import scala.collection.JavaConverters._
    val confAddressProps: Properties = new Properties()
    confAddressProps.load(classOf[Kafka2Iceberg].getClassLoader.getResourceAsStream("conf-address.properties"))
    val tableCfgArray = JobCfgHelper.getInstance.getConf(confKey, confAddressProps, useCfg).asScala

    /* 使用第一张表的配置来创建 SparkSession. */
    val headProp =  tableCfgArray.head.getCfgAsProperties
    val sparkConfigKeys = headProp.keySet().asScala.map(_.toString).filter(_.startsWith("spark"))
    val sparkConf = new SparkConf()
    for(key <- sparkConfigKeys){
      sparkConf.set(key, headProp.getProperty(key))
    }

    /*  Create SparkSession */
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    /* 创建 Spark Streaming Context 对象，并指定批次处理间隔. */
    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(batchDuration))

    /* debezium logical type register.  */
    LogicalTypes.register(TimestampZoned.TIMESTAMP_ZONED, new TimestampZonedFactory())

    /* 初始化 Schema 共享数据信息 （Broadcast / Accumulator） */
    statusAccumulatorMaps= HashMap[String, StatusAccumulator]()
    schemaBroadcastMaps= HashMap[String, Broadcast[SchemaBroadcast]]()
    for (tableCfg <- tableCfgArray) {
      /* 配置解析. */
      val props =  tableCfg.getCfgAsProperties
      val icebergTableName = props.getProperty(RunCfg.ICEBERG_TABLE_NAME).trim  /* Iceberg 表名 */
      val schemaVersion = props.getProperty(RunCfg.AVRO_SCHEMA_REGISTRY_VERSION) /* Avro Version. */
      /* 如果 Schema Version 未配置, 则从 Schema Registry Server 中读取最小的 Schema Version 作为初始化版本. */
      val initSchemaVersion =  if (schemaVersion == null || schemaVersion.trim.equals("")){
        SchemaUtils.getMinSchemaVersion(tableCfg)
      }else{
        schemaVersion.trim.toInt
      }

      /* 初始化 SchemaAccumulator 信息. */
      val statusAcc = StatusAccumulator.registerInstance(spark.sparkContext, icebergTableName)
      statusAcc.setSchemaVersion(initSchemaVersion)
      statusAccumulatorMaps += (icebergTableName -> statusAcc)
      /* 初始化 SchemaBroadcaster 信息. */
      val schemaToVersionMap: HashMap[String, Integer] = SchemaUtils.getSchemaToVersionMap(tableCfg)
      val versionToSchemaMap: HashMap[Integer, String] = SchemaUtils.getVersionToSchemaMap(tableCfg)
      val mergedSchemaStr: String = SchemaUtils.getInitMergedSchemaStr(tableCfg)
      val mergedStruct: StructType = SchemaUtils.convertSchemaToStructType(mergedSchemaStr)
      val schemaBroadcast = SchemaBroadcast(schemaToVersionMap, versionToSchemaMap, mergedSchemaStr, mergedStruct)
      schemaBroadcastMaps += (icebergTableName -> spark.sparkContext.broadcast(schemaBroadcast))
    }

    try {
      for (tableCfg <- tableCfgArray) {
        logInfo("JobConfig:" + tableCfg.toString)
        val cfg = new Properties()
        cfg.load(new StringReader(tableCfg.getConfValue))
        val bootstrapServers: String = cfg.getProperty(RunCfg.KAFKA_BOOTSTRAP_SERVERS)
        val schemaRegistryUrl: String = cfg.getProperty(RunCfg.KAFKA_SCHEMA_REGISTRY_URL)
        val groupId: String = cfg.getProperty(RunCfg.KAFKA_CONSUMER_GROUP_ID)
        val topics: Array[String] = cfg.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC).split(",")
        val maxPollRecords: String = cfg.getProperty(RunCfg.KAFKA_CONSUMER_MAX_POLL_RECORDS)
        val keyDeserializer: String = cfg.getProperty(RunCfg.KAFKA_CONSUMER_KEY_DESERIALIZER)
        val valueDeserializer: String = cfg.getProperty(RunCfg.KAFKA_CONSUMER_VALUE_DESERIALIZER)
        val icebergTable: String = cfg.getProperty(RunCfg.ICEBERG_TABLE_NAME)


        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> bootstrapServers,
          "schema.registry.url" -> schemaRegistryUrl,
          "group.id" -> groupId,
          "max.poll.records" -> maxPollRecords,
          "key.deserializer" -> keyDeserializer,
          "value.deserializer" -> valueDeserializer,
          "auto.offset.reset" -> "earliest",
          "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val stream = KafkaUtils.createDirectStream[String, GenericRecord](
          ssc,
          PreferConsistent,
          Subscribe[String, GenericRecord](topics, kafkaParams))

        stream.foreachRDD {
          rdd =>
            try {
              if (!rdd.isEmpty()) {
                logInfo("----------------------------------------------------------------------------------")
                val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                val  partitionOffsets = HashMap(offsetRanges.map(
                  offsetRange =>
                    (s"${offsetRange.topic}:${offsetRange.partition}", new PartitionOffset(
                      offsetRange.topic, offsetRange.partition, offsetRange.fromOffset,
                      offsetRange.untilOffset, offsetRange.fromOffset))
                ): _*)
                statusAccumulatorMaps(icebergTable).setPartitionOffsets(partitionOffsets)
                logInfo(s"OffsetRanges: [${offsetRanges.mkString(",")}]")
                logInfo(s"partitionOffsets: [${partitionOffsets.mkString(",")}]")

                IcebergWriter.write2(spark, rdd, tableCfg, useCfg)

                val commitOffsetRangers = statusAccumulatorMaps(icebergTable).getCommitOffsetRangers
                logInfo(s"commitOffsetRangers: [${commitOffsetRangers.mkString(",")}]")
            //    stream.asInstanceOf[CanCommitOffsets].commitAsync(commitOffsetRangers)
                logInfo("----------------------------------------------------------------------------------\n")
              } else {
                logInfo(s"table streaming rdd is empty ...")
              }
            }catch {
              case e: Exception =>
                logError("found unknown exception when write, stop job...")
                e.printStackTrace()
                ssc.stop(stopSparkContext = true, stopGracefully = true)
                throw e
            }
        }
      }
      ssc.start()
      ssc.awaitTermination()
    } catch {
      case e: Exception =>
        logError("Found unknown exception , stop job...")
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        throw e
    }



  }

}
