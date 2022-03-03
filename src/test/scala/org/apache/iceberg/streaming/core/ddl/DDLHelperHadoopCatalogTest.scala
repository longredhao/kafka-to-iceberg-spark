package org.apache.iceberg.streaming.core.ddl

import org.junit.Test
import org.junit.Assert.assertFalse
import org.junit.Assert.fail

import java.io.IOException
import java.io.InputStream
import java.sql.Timestamp
import java.sql.Types
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.Collections
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors
import java.util.stream.Stream
import org.junit.Before
import org.junit.Test

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`



class DDLHelperHadoopCatalogTest extends org.scalatest.FunSuite {
  @Test
  def t1(): Unit = {
    import org.apache.spark.sql.{Row, SparkSession}
    import org.apache.iceberg.hive.HiveCatalog
    import org.apache.iceberg.{CatalogUtil, PartitionSpec}
    import org.apache.iceberg.avro.{AvroSchemaUtil, AvroSchemaVisitor, SchemaToType}
    import org.apache.avro.Schema.Parser
    import org.apache.iceberg.catalog.TableIdentifier
    import org.apache.iceberg.catalog.Namespace
    import org.apache.iceberg.types.{CheckCompatibility, Types}
    import org.apache.kafka.clients.admin.AdminClient
    import org.apache.kafka.clients.consumer.internals.SubscriptionState
    import org.apache.spark.streaming.kafka010.ConsumerStrategies

    import java.util
    import org.apache.thrift.TException;
    import org.apache.commons.lang.StringUtils;

    /**
     * 创建 SparkSession 指定 Hive metastore 参数
     */
    val spark = SparkSession.builder().
      master("local[2]").
      config("spark.sql.sources.partitionOverwriteMode", "dynamic").
      config("spark.sql.extensions" , "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").
      config("spark.sql.catalog.hive" , "org.apache.iceberg.spark.SparkCatalog").
      config("spark.sql.catalog.hive.type" , "hive").
      config("spark.hadoop.hive.metastore.uris" , "thrift://localhost:9083").
      enableHiveSupport().appName("Kafka2Iceberg").getOrCreate()
    spark.sql("show databases").show
    spark.sql("select * from hive.db.tbl_test").show


//    /**
//     * 创建 catalog
//     */
//    val catalog = new HiveCatalog();
//    catalog.setConf(spark.sparkContext.hadoopConfiguration);  // Configure using Spark's Hadoop configuration
//    val properties = new util.HashMap[String, String]()
//    properties.put("warehouse", "hdfs://hadoop:8020/user/hive/warehouse/iceberg");
//    properties.put("uri", "thrift://hadoop:9083");
//    catalog.initialize("hive", properties);

    /**
     * 使用 catalog 创建表
     */
    val schemaStr1 ="""
                      |{
                      |  "type": "record",
                      |  "name": "Envelope",
                      |  "namespace": "test.db_gb18030_test.tbl_test",
                      |  "fields": [
                      |    {"name": "id", "type": ["null", "int"], "default": null},
                      |    {"name": "name", "type": ["null", "string"], "default": null},
                      |    {"name": "age", "type": ["null", "int"], "default": null},
                      |    {"name": "_op", "type": ["null", "string"], "default": null}
                      |  ]
                      |}
                      |""".stripMargin
    val schema1 = new Parser().parse(schemaStr1)
    val shadedSchema1 =  new org.apache.avro.Schema.Parser().parse(schema1.toString())
    val icebergSchema1 = AvroSchemaUtil.toIceberg(shadedSchema1);

//
//    val db = Namespace.of("db")
//    if (!catalog.namespaceExists(db)) catalog.createNamespace(db)

    val icebergTableName = "hive.db.sample"
    val partitionBy = "name"
    val location = "'hdfs://hadoop:8020/user/hive/warehouse/iceberg-spark-1'"
    val comment = "'hive.db.sample'"
    val tblProperties = "'read.split.target-size'='268435456'"

    val columnArr =  icebergSchema1.columns().map(c => s"${c.name()} ${c.`type`()}")
    val createDDL =
      s"""CREATE TABLE $icebergTableName (
         |${columnArr.mkString(", ")})
         |USING iceberg
         |PARTITIONED BY ($partitionBy)
         |LOCATION $location
         |COMMENT $comment
         |TBLPROPERTIES ($tblProperties)
         |""".stripMargin

    System.out.println(createDDL)
    spark.sql(createDDL)
    spark.sql(s"select * from $icebergTableName").show








    spark.stop()
     }

}
