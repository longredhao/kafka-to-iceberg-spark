package org.apache.iceberg.streaming


import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.streaming.avro.SchemaUtils
import org.apache.iceberg.streaming.config.{Config, JobCfgHelper, RunCfg, TableCfg}
import org.apache.iceberg.streaming.write.IcebergWriter
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.iceberg.streaming.core.accumulator.{PartitionOffset, StatusAccumulator}
import org.apache.iceberg.streaming.core.broadcast
import org.apache.iceberg.streaming.core.broadcast.SchemaBroadcast
import org.apache.iceberg.streaming.exception.SchemaChangedException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream

import java.io.StringReader
import java.util.Properties
import scala.collection.immutable.HashMap



class Kafka2Iceberg {
}

object Kafka2Iceberg extends Logging{

  /* Schema 共享数据信息 （Broadcast / Accumulator). */
  var statusAccumulatorMaps: HashMap[String, StatusAccumulator] = _
  var schemaBroadcastMaps: HashMap[String, Broadcast[SchemaBroadcast]] = _

  /* 是否重启 Steaming 流(当数据 Schema 发生变动时需重启，以重复消费被上个 batch 丢弃的新 Schema Version 数据) */
  var restartStream: Boolean = _
  var stopStream: Boolean = _

  /* Spark Context */
  var spark: SparkSession = _
  var ssc: StreamingContext = _


  /**
   * 主函数入口.
   * @param args Array[String]
   */
  def main(args: Array[String]): Unit = {
    /* User Input Config */
    val useCfg = Config.getInstance().loadConf(args).toProperties
    val confKey: String = useCfg.getProperty("confKey")
    val batchDuration = useCfg.getProperty("batchDuration").toInt
    val runEnv = useCfg.getProperty("runEnv", RunCfg.RUN_ENV_DEFAULT) /* test or product. */

    /* Load Job Config Form Config Database */
    import scala.collection.JavaConverters._
    val confAddressProps: Properties = new Properties()
    confAddressProps.load(classOf[Kafka2Iceberg].getClassLoader.getResourceAsStream("conf-address.properties"))
    val cfgArr = JobCfgHelper.getInstance.getConf(confKey, confAddressProps, runEnv).asScala.toArray

    /* 使用第一张表的配置来创建 SparkSession. */
    val headProp =  cfgArr.head.getCfgAsProperties
    val sparkConfigKeys = headProp.keySet().asScala.map(_.toString).filter(_.startsWith("spark"))
    val sparkConf = new SparkConf()
    for(key <- sparkConfigKeys){
      sparkConf.set(key, headProp.getProperty(key))
    }
    spark = SparkSession.builder().config(sparkConf).getOrCreate()
    /* 创建 Spark Streaming Context 对象，并指定批次处理间隔. */
    ssc = new StreamingContext(spark.sparkContext, Seconds(batchDuration))


    /* 初始化共享变量 */
    initAccumulatorAndBroadcast(spark, cfgArr)
    restartStream = false  /* 当检测到 Schema 发生变化时重启, 回溯消费位点以重复处理被上次 batch 作业被丢弃的数据 */
    stopStream = false
    logInfo(s"After Init StatusAccumulator [${statusAccumulatorMaps.toString}]")
    logInfo(s"After Init SchemaBroadcast   [${schemaBroadcastMaps.map(x => s"SchemaBroadcast(${x._1}, ${x._2.value.toString})")}]")


    while(!stopStream){
      try {

        for (cfg <- cfgArr) {
          startStreamJob(cfg, useCfg)
          val icebergTable: String = cfg.getCfgAsProperties.getProperty(RunCfg.ICEBERG_TABLE_NAME)
          if (statusAccumulatorMaps(icebergTable).isAllPartSchemaChanged) {
          }

          ssc.start()
          ssc.awaitTermination()
        }
      } catch {
        case _: SchemaChangedException =>
          logWarning("Detect Schema Version Changed, Restart SparkStream Job...")
          ssc.stop(stopSparkContext = false, stopGracefully = false)  /* 强制停止, 否则会继续执行被 Schedule 的 Batch */
          ssc = new StreamingContext(spark.sparkContext, Seconds(batchDuration))
          logInfo("Restarted StreamingContext ...")

          /* 由于 SparkStream 仅支持异步提交 Kafka Commit Offset, 因此需要强制同步 Commit Offset. */
          for (cfg <- cfgArr) {
            val icebergTable: String = cfg.getCfgAsProperties.getProperty(RunCfg.ICEBERG_TABLE_NAME)
            kafka.KafkaUtils.commitAsync(cfg, statusAccumulatorMaps(icebergTable).convertToKafkaCommitOffset())
          }

          /* 重新初始化共享变量 */
          logInfo("Restarted initAccumulatorAndBroadcast ...")
          initAccumulatorAndBroadcast(spark, cfgArr)
          restartStream = false  /* 当检测到 Schema 发生变化时重启, 回溯消费位点以重复处理被上次 batch 作业被丢弃的数据 */
          stopStream = false
          logInfo(s"After Init StatusAccumulator [${statusAccumulatorMaps.toString}]")
          logInfo(s"After Init SchemaBroadcast   [${schemaBroadcastMaps.map(x => s"SchemaBroadcast(${x._1}, ${x._2.value.toString})")}]")

        case e: Exception =>
          logError("Found unknown exception , stop job...")
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          stopStream=true
          throw e
      }
    }

    spark.stop()
  }




  def startStreamJob(cfg: TableCfg, useCfg: Properties): Unit = {
    val prop = new Properties()
    prop.load(new StringReader(cfg.getConfValue))
    val bootstrapServers: String = prop.getProperty(RunCfg.KAFKA_BOOTSTRAP_SERVERS)
    val schemaRegistryUrl: String = prop.getProperty(RunCfg.KAFKA_SCHEMA_REGISTRY_URL)
    val groupId: String = prop.getProperty(RunCfg.KAFKA_CONSUMER_GROUP_ID)
    val topics: Array[String] = prop.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC).split(",")
    val maxPollRecords: String = prop.getProperty(RunCfg.KAFKA_CONSUMER_MAX_POLL_RECORDS)
    val keyDeserializer: String = prop.getProperty(RunCfg.KAFKA_CONSUMER_KEY_DESERIALIZER)
    val valueDeserializer: String = prop.getProperty(RunCfg.KAFKA_CONSUMER_VALUE_DESERIALIZER)
    val icebergTable: String = prop.getProperty(RunCfg.ICEBERG_TABLE_NAME)

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

    val stream: InputDStream[ConsumerRecord[String, GenericRecord]] =
      KafkaUtils.createDirectStream[String, GenericRecord](ssc, PreferConsistent,
        Subscribe[String, GenericRecord](topics, kafkaParams))

    stream.foreachRDD {
      rdd =>
        if (!rdd.isEmpty()) {
          logInfo("----------------------------------------------------------------------------------")
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          val  partitionOffsets = HashMap(offsetRanges.map(
            offsetRange =>
              (s"${offsetRange.topic}:${offsetRange.partition}", new PartitionOffset(
                offsetRange.topic, offsetRange.partition, offsetRange.fromOffset,
                offsetRange.untilOffset, offsetRange.fromOffset))
          ): _*)

          /* 更新 PartitionOffset 状态（ 用于后续 untilOffset 和 curOffset 进行对比判断数据 Schema 是否存在更新）*/
          statusAccumulatorMaps(icebergTable).updatePartitionOffsets(partitionOffsets)
          logInfo(s"OffsetRanges: [${offsetRanges.mkString(",")}]")
          logInfo(s"partitionOffsets: [${partitionOffsets.mkString(",")}]")

          IcebergWriter.write(spark, rdd, cfg, useCfg)

          /* 提交 Kafka Offset */
          val commitOffsetRangers = statusAccumulatorMaps(icebergTable).getCommitOffsetRangers
          logInfo(s"commit offset rangers: [${commitOffsetRangers.mkString(",")}]")
          stream.asInstanceOf[CanCommitOffsets].commitAsync(commitOffsetRangers)
          logInfo("----------------------------------------------------------------------------------\n")

          /* Schema 更新检测： 如果所有分区的 Schema 发生改变，更新 Schema 版本，并重启 Stream 流作业 */
          val statusAcc = statusAccumulatorMaps(icebergTable)
          if (statusAcc.isAllPartSchemaChanged) {
            /* 更新 Schema Version */
            statusAcc.upgradeSchemaVersion(1)
            logInfo(s"upgrade schema version to ${statusAcc.schemaVersion}")
            /* 抛出 Schema 版本更新异常, 结束 Stream 作业, 然后捕捉异常并重启... */
            throw new SchemaChangedException("detect schema version changed, need restart SparkStream job...")
          }
        } else {
          logInfo(s"table streaming rdd is empty ...")
        }
    }

    }


  /**
   * 初始化共享变量
   */
  def initAccumulatorAndBroadcast(spark: SparkSession, cfgArr: Array[TableCfg]): Unit = {
    /* 初始化 Schema 共享数据信息 （Broadcast / Accumulator） */
    statusAccumulatorMaps= HashMap[String, StatusAccumulator]()
    schemaBroadcastMaps= HashMap[String, Broadcast[SchemaBroadcast]]()
    for (cfg <- cfgArr) {
      /* 配置解析. */
      val props =  cfg.getCfgAsProperties
      val icebergTableName = props.getProperty(RunCfg.ICEBERG_TABLE_NAME).trim  /* Iceberg 表名 */

      /* 初始化 StatusAccumulator 信息, 通过 Kafka Committed Offset 获取历史的 Schema Version 信息 */
      val schemaVersion = kafka.KafkaUtils.getCurrentSchemaVersion(cfg) /* Avro Version. */
      val statusAcc = StatusAccumulator.registerInstance(spark.sparkContext, icebergTableName)
      statusAcc.setSchemaVersion(schemaVersion)
      statusAcc.initPartitionOffset(cfg)
      statusAccumulatorMaps += (icebergTableName -> statusAcc)

      /* 初始化 SchemaBroadcaster 信息. */
      val schemaToVersionMap: HashMap[Schema, Integer] = SchemaUtils.getSchemaToVersionMap(cfg)
      val versionToSchemaMap: HashMap[Integer, Schema] = SchemaUtils.getVersionToSchemaMap(cfg)
      val schemaBroadcast = broadcast.SchemaBroadcast(schemaToVersionMap, versionToSchemaMap)
      schemaBroadcastMaps += (icebergTableName -> spark.sparkContext.broadcast(schemaBroadcast))
    }
  }

}
