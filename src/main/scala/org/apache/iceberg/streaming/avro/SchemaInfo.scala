//package org.apache.iceberg.streaming.avro
//
//import io.confluent.kafka.schemaregistry.avro.AvroSchema
//import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
//import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
//import org.apache.avro.Schema
//import org.apache.spark.internal.Logging
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.StructType
//
//import scala.collection.immutable.HashMap
//
//case class SchemaInfo(
//                       schemaToVersionMap: HashMap[String, Integer],
//                       versionToSchemaMap: HashMap[Integer, String]
//                     )  extends  Serializable {
//}
//
///**
// * schemaVersionMaps: 历史 Avro Schema Version.
// */
//object SchemaInfo extends Logging {
//
//  /* Map 映射： Save Path ->  Avro Schema Version, 用于记录当前的 Avro Schema Version . */
//  @volatile var schemaVersionMaps: HashMap[String, SchemaVersionAccumulator] = _
//
//  /* Map 映射： Save Path -> Schema Merged StructType, 用于校验添加被删除的列. */
//  @volatile var schemaInfoMaps: HashMap[String, SchemaInfo] = _
//
//  @volatile var schemaMergedStructMaps: HashMap[String, StructType] = _
//
//  /**
//   * loadSchemaInfo.
//   *
//   * @param jobCfg JobConfig
//   * @return SchemaInfo
//   */
//  def loadSchemaInfo(jobCfg: JobConfig): SchemaInfo = {
//    val kafkaCfg = jobCfg.getKafkaCfg
//    val topic = kafkaCfg.getStringValue(RunConfig.KAFKA_CONSUMER_TOPIC)
//    val client: SchemaRegistryClient = SchemaInfo.getSchemaRegistryClientInstance(jobCfg)
//    val versions: java.util.List[Integer] = client.getAllVersions(topic + "-value")
//    var schemaToVersion: HashMap[String, Integer] = HashMap()
//    var versionToSchema: HashMap[Integer, String] = HashMap()
//    for (i <- 0 until versions.size()) {
//      val schema = client.getByVersion(topic + "-value", versions.get(i), false).getSchema
//      schemaToVersion += (schema -> versions.get(i))
//      versionToSchema += (versions.get(i) -> schema)
//    }
//    SchemaInfo(schemaToVersion, versionToSchema)
//  }
//
//  /**
//   * Get Schema Registry Client Instance.
//   *
//   * @param jobCfg JobConfig
//   * @return SchemaRegistryClient
//   */
//  def getSchemaRegistryClientInstance(jobCfg: JobConfig): SchemaRegistryClient = {
//    getSchemaRegistryClientInstance(jobCfg.getKafkaCfg)
//  }
//
//  /**
//   * Get Schema Registry Client Instance.
//   *
//   * @param kafkaCfg kafkaCfg
//   * @return SchemaRegistryClient
//   */
//  def getSchemaRegistryClientInstance(kafkaCfg: Config): SchemaRegistryClient = {
//    val schemaRegistryUrl = kafkaCfg.getStringValue(RunConfig.KAFKA_SCHEMA_REGISTRY_URL)
//    new CachedSchemaRegistryClient(schemaRegistryUrl, AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)
//  }
//
//
//  /**
//   * Load Schema Fields From Hoodie Table.
//   *
//   * @param spark           SparkSession
//   * @param hoodieTablePath Table Save Path
//   * @return Schema Fields Column Name
//   */
//  def loadMergedStructFromHoodieTable(spark: SparkSession, hoodieTablePath: String): StructType = {
//    StructType.apply(spark.read.option("mergeSchema", "true").format("hudi").
//      load(hoodieTablePath + "/*/*").schema.fields.filter(!_.name.startsWith("_hoodie")).toList)
//  }
//
//  /**
//   * Load Avro Schema From Schema Registry Server.
//   *
//   * @param kafkaCfg Config
//   * @return Schema
//   */
//  def loadSchemaRegistryInitSchema(kafkaCfg: Config): Schema = {
//    val topic = kafkaCfg.getStringValue(RunConfig.KAFKA_CONSUMER_TOPIC)
//    val client: SchemaRegistryClient = SchemaInfo.getSchemaRegistryClientInstance(kafkaCfg)
//    val initVersion = kafkaCfg.getIntValue(RunConfig.SCHEMA_AVRO_INIT_VERSION, client.getAllVersions(s"$topic-value").toArray.head.toString.toInt)
//    val schemaJson = client.getByVersion(s"$topic-value", initVersion, false).getSchema
//    new Schema.Parser().parse(schemaJson)
//  }
//
//  /**
//   * getSchemaVersion.
//   * deprecated 要求 Avro 版本 1.9.2+ 与 Spark 2.4版本不兼容
//   *
//   * @param kafkaCfg Config
//   * @return Schema
//   */
//  @deprecated
//  def getSchemaVersion(schema: Schema, kafkaCfg: Config): Int = {
//    val topic = kafkaCfg.getStringValue(RunConfig.KAFKA_CONSUMER_TOPIC)
//    val client: SchemaRegistryClient = SchemaInfo.getSchemaRegistryClientInstance(kafkaCfg)
//    client.getVersion(topic, new AvroSchema(schema))
//  }
//
//
//  /**
//   * getSchemaVersion.
//   * deprecated 要求 Avro 版本 1.9.2+ 与 Spark 2.4版本不兼容
//   *
//   * @param schema String
//   * @param kafkaCfg    Config
//   * @return Schema Version
//   */
//  @deprecated
//  def getSchemaVersion(schema: String, kafkaCfg: Config): Int = {
//    val topic = kafkaCfg.getStringValue(RunConfig.KAFKA_CONSUMER_TOPIC)
//    val client: SchemaRegistryClient = SchemaInfo.getSchemaRegistryClientInstance(kafkaCfg)
//    client.getVersion(topic, new AvroSchema(schema))
//  }
//
//
//  /**
//   * 从 Schema Register Server 加载 Schema 到 Version 的映射.
//   *
//   * @param cfg JobConfig
//   * @return schemaVersionMap
//   */
//  def loadSchemaVersionMap(cfg: JobConfig): HashMap[String, Long] = {
//    val kafkaCfg = cfg.getKafkaCfg
//    val topic = kafkaCfg.getStringValue(RunConfig.KAFKA_CONSUMER_TOPIC)
//    val client: SchemaRegistryClient = SchemaInfo.getSchemaRegistryClientInstance(kafkaCfg)
//    val versions: java.util.List[Integer] = client.getAllVersions(topic + "-value")
//    var schemaVersionMap: HashMap[String, Long] = HashMap()
//    for (i <- 0 until versions.size()) {
//      schemaVersionMap += (client.getByVersion(topic + "-value", versions.get(i), false).getSchema -> versions.get(i).toLong)
//    }
//    schemaVersionMap
//  }
//
//}
//
