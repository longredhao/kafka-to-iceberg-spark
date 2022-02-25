package org.apache.iceberg.streaming.avro.schema

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.Schema
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.streaming.avro.{SchemaConverters, SchemaFixLogicalType}
import org.apache.iceberg.streaming.config.{RunCfg, TableCfg}
import org.apache.spark.sql.types.StructType

import java.util
import java.util.List

import scala.collection.immutable.HashMap

/**
 * Schema 工具类
 */
object SchemaUtils {

  /**
   * Get Schema Registry Client Instance.
   *
   * @param tableCfg TableCfg
   * @return SchemaRegistryClient
   */
  def getSchemaRegistryClientInstance(tableCfg: TableCfg): SchemaRegistryClient = {
    val schemaRegistryUrl = tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_SCHEMA_REGISTRY_URL)
    getSchemaRegistryClientInstance(schemaRegistryUrl,AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)
  }


  /**
   * Get Schema Registry Client Instance.
   *
   * @return SchemaRegistryClient
   */
  def getSchemaRegistryClientInstance(schemaRegistryUrl: String, cacheCapacity: Int): SchemaRegistryClient = {
    new CachedSchemaRegistryClient(schemaRegistryUrl, cacheCapacity)
  }

  /**
   * Get Schema Registry Client Instance.
   *
   * @return SchemaRegistryClient
   */
  def getSchemaRegistryClientInstance(schemaRegistryUrl: String): SchemaRegistryClient = {
    new CachedSchemaRegistryClient(schemaRegistryUrl, AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT )
  }



  /**
   * 从 Schema Registry Server 中加载最小的 Schema Version
   * @param tableCfg TableCfg
   * @return Schema Version
   */
  def getMinSchemaVersion(tableCfg: TableCfg): Int = {
    val topic =  tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC)
    val client: SchemaRegistryClient = getSchemaRegistryClientInstance(tableCfg)
    val versions: java.util.List[Integer] = client.getAllVersions(topic + "-value")
    versions.get(0)
  }

  /**
   * 从 Schema Registry Server 中加载 Schema -> Schema Version 的映射关系.
   * @param tableCfg TableCfg
   * @return HashMap[String, Integer]
   */
  def getSchemaToVersionMap(tableCfg: TableCfg): HashMap[String, Integer] = {
    val topic =  tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC)
    val client: SchemaRegistryClient = getSchemaRegistryClientInstance(tableCfg)
    val versions: java.util.List[Integer] = getSchemaVersionList(tableCfg)
    var schemaToVersion: HashMap[String, Integer] = HashMap()
    for (i <- 0 until versions.size()) {
      val schema = client.getByVersion(topic + "-value", versions.get(i), false).getSchema
      schemaToVersion += (schema -> versions.get(i))
    }
     schemaToVersion
  }

  /**
   * 从 Schema Registry Server 中加载 Schema Version -> Schema 的映射关系.
   * @param tableCfg TableCfg
   * @return
   */
  def getVersionToSchemaMap(tableCfg: TableCfg): HashMap[Integer, String] = {
    val topic =  tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC)
    val client: SchemaRegistryClient = getSchemaRegistryClientInstance(tableCfg)
    val versions: java.util.List[Integer] = getSchemaVersionList(tableCfg)
    var versionToSchema: HashMap[Integer, String] = HashMap()
    for (i <- 0 until versions.size()) {
      val schema = client.getByVersion(topic + "-value", versions.get(i), false).getSchema
      versionToSchema += (versions.get(i) -> schema)
    }
    versionToSchema
  }

  /**
   *  从 Schema Registry Server 中加载 所有的 Schema Version
   * @param tableCfg TableCfg
   * @return  Schema Version List
   */
  def getSchemaVersionList(tableCfg: TableCfg):java.util.List[Integer] = {
    val topic =  tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC)
    val client: SchemaRegistryClient = getSchemaRegistryClientInstance(tableCfg)
    client.getAllVersions(topic + "-value")
  }


  def getSchemaByVersion(tableCfg: TableCfg, schemaVersion: Int): String = {
    val topic =  tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC)
    val client: SchemaRegistryClient = getSchemaRegistryClientInstance(tableCfg)
    client.getByVersion(topic + "-value", schemaVersion, false).getSchema

  }



  /**
   * 获取 初始化 Merged Schema 信息
   * 1、如果配置中未找到 avro.schema.merged.string 配置, 则认定为第一次执行, 从 Schema Registry Server 中读取最小的 Schema 版本信息
   * 2、如果配置中存在 avro.schema.merged.string, 则解析配置并返回
   * @param tableCfg TableCfg
   * @return Init Avro Schema
   */
  def getInitMergedSchemaStr(tableCfg: TableCfg): String = {
    val cfgSchemaStr=  tableCfg.getCfgAsProperties.getProperty(RunCfg.AVRO_SCHEMA_MERGED_STRING)
    val schemaStr =
      if(cfgSchemaStr == null || cfgSchemaStr.trim.equals("")){
        val initSchemaVersion = getMinSchemaVersion(tableCfg)
        getSchemaByVersion(tableCfg, initSchemaVersion)
      }else{
        cfgSchemaStr
      }
    schemaStr
  }

  /**
   * 将当前版本的 Schema 信息合并更新入历史的 His Merged Schema 中
   * @param hisMergedSchemaStr 历史所有 Schema 版本合并信息
   * @param curAvroSchemaStr 前版本的 Schema 信息
   * @return
   */
  def getUpdateMergedSchemaStr(hisMergedSchemaStr: String, curAvroSchemaStr: String): String = {
    /* 历史所有 Schema 版本合并信息 */
    val hisMergedSchema = new Schema.Parser().parse(hisMergedSchemaStr)
    val hisMergedFields: util.List[Schema.Field] =  hisMergedSchema.getFields

    /* 前版本的 Schema 信息 */
    val curAvroSchema = new Schema.Parser().parse(curAvroSchemaStr)
    val curAvroFields: util.List[Schema.Field]  =  curAvroSchema.getFields

    /* 添加当前版本 Schema 中新增的 Schema.Field 列到 Merged Schema 版本信息中 */
    for(i <- 0 until curAvroFields.size()){
      val field = curAvroFields.get(i)
      if(!hisMergedFields.contains(field.schema())){
        hisMergedFields.add(field)
      }
    }

    /* 返回最新的 Schema 合并结果 */
    val newMergedSchema = Schema.createRecord(hisMergedFields)
    newMergedSchema.toString()
  }

  /**
   * 获取 初始化 Schema 信息 - 并根据 SchemaConverters  转换为Spark StructType 类型
   * @param tableCfg TableCfg
   * @return StructType
   */
  @deprecated("instead by convertSchemaToStructType")
  def getInitMergedStructV1(tableCfg: TableCfg): StructType = {
    val schema = getInitMergedSchemaStr(tableCfg)
    val targetSchema = SchemaFixLogicalType.fixLogicalType(schema)
    SchemaConverters.toSqlType(targetSchema).dataType.asInstanceOf[StructType]
  }

  /**
   * 获取 初始化 Schema 信息 -
   * 由于现有接口不支持从 Avro Schema 直接转换为 Spark StructType 类型
   * 首先并根据 [[AvroSchemaUtil]] 将 Avro Schema 间接转换为 iceberg schema
   * 最后使用 [[org.apache.iceberg.spark.SparkSchemaUtil]] 将  iceberg schema 转换为 Spark StructType 类型
   * @param tableCfg TableCfg
   * @return StructType
   */
  @deprecated("instead by convertSchemaToStructType ")
  def getInitMergedStructV2(tableCfg: TableCfg): StructType = {
    val schema = getInitMergedSchemaStr(tableCfg)
    convertSchemaToStructType(schema)
  }

  /**
   * 由于现有接口不支持从 Avro Schema 直接转换为 Spark StructType 类型
   * 首先并根据 [[AvroSchemaUtil]] 将 Avro Schema 间接转换为 iceberg schema
   * 最后使用 [[org.apache.iceberg.spark.SparkSchemaUtil]] 将  iceberg schema 转换为 Spark StructType 类型
   * @param schema Stranded Avro Schema
   * @return StructType
   */
  def convertSchemaToStructType(schema: Schema): StructType = {
    org.apache.iceberg.spark.SparkSchemaUtil.convert(AvroSchemaUtil.toIceberg(schema))
  }

  /**
   * 由于现有接口不支持从 Avro Schema 直接转换为 Spark StructType 类型
   * 首先并根据 [[AvroSchemaUtil]] 将 Avro Schema 间接转换为 iceberg schema
   * 最后使用 [[org.apache.iceberg.spark.SparkSchemaUtil]] 将  iceberg schema 转换为 Spark StructType 类型
   * @param schemaStr Stranded Avro Schema Json String
   * @return StructType
   */
  def convertSchemaToStructType(schemaStr: String): StructType = {
    convertSchemaToStructType(new Schema.Parser().parse(schemaStr))
  }


  /**
   * 获取 Metadata 的 Source 域中需要解析的字段的 index
   * @param tableCfg TableCfg
   * @param schema Original Avro Schema
   * @return
   */
  def getSourceFieldIndex(tableCfg: TableCfg, schema: Schema): Seq[Int] = {
    val metadataSourceCols = tableCfg.getCfgAsProperties.getProperty(RunCfg.RECORD_METADATA_SOURCE_COLUMNS).split(",").map(_.trim)
    val sourceSchema = schema.getField("source").schema()
    metadataSourceCols.map(x =>  sourceSchema.getField(x).pos())
  }

  def getTransactionIndex(tableCfg: TableCfg, schema: Schema): Seq[Int] = {
    val metadataTransCols = tableCfg.getCfgAsProperties.getProperty(RunCfg.RECORD_METADATA_TRANSACTION_COLUMNS).split(",").map(_.trim)
    val transactionSchema = schema.getField("transaction").schema()
    val index = transactionSchema.getIndexNamed("io.confluent.connect.avro.ConnectDefault")
    val connectTransSchema = transactionSchema.getTypes.get(index)
    metadataTransCols.map(x =>  connectTransSchema.getField(x).pos())
  }



}
