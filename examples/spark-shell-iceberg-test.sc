
/**
 * Start spark-shell
 * /opt/run/spark3/bin/spark-shell --packages org.apache.iceberg:iceberg-spark3-runtime:0.13.0
 *
 * /opt/run/spark3/bin/spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.1
 */

spark.stop()

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.{CatalogUtil, PartitionSpec}
import org.apache.iceberg.avro.{AvroSchemaUtil, AvroSchemaVisitor, SchemaToType}
import org.apache.avro.Schema.Parser
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.types.Types
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.internals.SubscriptionState
import org.apache.spark.streaming.kafka010.ConsumerStrategies

import java.util

/**
 * 创建 SparkSession 指定 Hive metastore 参数
 */
val spark = SparkSession.builder().
  master("local[2]").
  config("spark.sql.sources.partitionOverwriteMode", "dynamic").
  config("spark.sql.extensions" , "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").
  config("spark.sql.catalog.hive" , "org.apache.iceberg.spark.SparkCatalog").
  config("spark.sql.catalog.hive.type" , "hive").
  config("spark.hadoop.hive.metastore.uris" , "thrift://hadoop:9083").
  enableHiveSupport().appName("Kafka2Iceberg").getOrCreate()
spark.sql("show databases").show


/**
 * 创建 catalog
 */
val catalog = new HiveCatalog();
catalog.setConf(spark.sparkContext.hadoopConfiguration);  // Configure using Spark's Hadoop configuration
val properties = new util.HashMap[String, String]()
properties.put("warehouse", "hdfs://hadoop:8020/user/hive/warehouse/iceberg");
properties.put("uri", "thrift://hadoop:9083");
catalog.initialize("hive", properties);

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

val db = Namespace.of("db")
if (!catalog.namespaceExists(db)) catalog.createNamespace(db)

val spec = PartitionSpec.builderFor(icebergSchema1).identity("id").build();
val tableName = TableIdentifier.of("db", "tbl_test");
val db_tbl_test = catalog.createTable(tableName, icebergSchema1, spec)



/**
 * 数据写入测试
 */
import spark.implicits._
val df1 = Seq(
  (1, "Karol", 19, "r"),
  (2, "Abby", 18, "r"),
  (3, "Zena", 20, "r")
).toDF("id", "name", "age", "_op")
df1.createOrReplaceTempView("origin_table")
val writeSql =
  """
    |MERGE INTO hive.db.tbl_test as t
    |USING (SELECT * from origin_table) as s
    |ON t.id = s.id
    |WHEN MATCHED AND s._op = 'd' THEN DELETE
    |WHEN MATCHED AND s._op = 'u' THEN UPDATE SET *
    |WHEN NOT MATCHED THEN INSERT *
    |""".stripMargin
spark.sql(writeSql)
spark.sql("select * from hive.db.tbl_test").show



/**
 * 数据更新测试
 */
val df2 = Seq(
  (4, "Karol", 21, "i"),
  (2, "Abby", 19, "u"),
  (3, "Zena", 20, "d")
).toDF("id", "name", "age", "_op")
df2.createOrReplaceTempView("update_table1")
val writeSql =
  """
    |MERGE INTO hive.db.tbl_test as t
    |USING (SELECT * from update_table1) as s
    |ON t.id = s.id
    |WHEN MATCHED AND s._op = 'd' THEN DELETE
    |WHEN MATCHED AND s._op = 'u' THEN UPDATE SET *
    |WHEN NOT MATCHED THEN INSERT *
    |""".stripMargin
spark.sql(writeSql)
spark.sql("select * from hive.db.tbl_test").show

/**
 * 表结构更新测试-增加列
 */
val schemaStr2 ="""
                 |{
                 |  "type": "record",
                 |  "name": "Envelope",
                 |  "namespace": "test.db_gb18030_test.tbl_test",
                 |  "fields": [
                 |    {"name": "id", "type": ["null", "int"], "default": null},
                 |    {"name": "name", "type": ["null", "string"], "default": null},
                 |    {"name": "age", "type": ["null", "int"], "default": null},
                 |    {"name": "score", "type": ["null", "int"], "default": null},
                 |    {"name": "_op", "type": ["null", "string"], "default": null}
                 |  ]
                 |}
                 |""".stripMargin
val schema2 = new Parser().parse(schemaStr2)
val shadedSchema2 =  new org.apache.avro.Schema.Parser().parse(schema2.toString())
val icebergSchema2 = AvroSchemaUtil.toIceberg(shadedSchema2)

db_tbl_test.updateSchema().unionByNameWith(icebergSchema2).commit()


// db_tbl_test.updateSchema().addColumn("score", Types.IntegerType.get()).commit()

val df3 = Seq(
  (5, "Karol", 23, 80, "i"),
  (4, "Abby", 22, 81, "u"),
).toDF("id", "name", "age", "score","_op")
df3.createOrReplaceTempView("update_table2")
val writeSql =
  """
    |MERGE INTO hive.db.tbl_test as t
    |USING (SELECT * from update_table2) as s
    |ON t.id = s.id
    |WHEN MATCHED AND s._op = 'd' THEN DELETE
    |WHEN MATCHED AND s._op = 'u' THEN UPDATE SET *
    |WHEN NOT MATCHED THEN INSERT *
    |""".stripMargin
spark.sql(writeSql)
spark.sql("select * from hive.db.tbl_test").show



/**
 * 表结构更新测试-删除列
 */
val schemaStr3 ="""
                  |{
                  |  "type": "record",
                  |  "name": "Envelope",
                  |  "namespace": "test.db_gb18030_test.tbl_test",
                  |  "fields": [
                  |    {"name": "id", "type": ["null", "int"], "default": null},
                  |    {"name": "name", "type": ["null", "string"], "default": null},
                  |    {"name": "score", "type": ["null", "int"], "default": null},
                  |    {"name": "_op", "type": ["null", "string"], "default": null}
                  |  ]
                  |}
                  |""".stripMargin
val schema3 = new Parser().parse(schemaStr3)
val shadedSchema3 =  new org.apache.avro.Schema.Parser().parse(schema3.toString())
val icebergSchema3 = AvroSchemaUtil.toIceberg(shadedSchema3)





db_tbl_test.updateSchema().unionByNameWith(icebergSchema3).commit()

// db_tbl_test.updateSchema().addColumn("score", Types.IntegerType.get()).commit()

val df3 = Seq(
  (5, "Karol", 23, 80, "i"),
  (4, "Abby", 22, 81, "u"),
).toDF("id", "name", "age", "score","_op")
df3.createOrReplaceTempView("update_table3")
val writeSql =
  """
    |MERGE INTO hive.db.tbl_test as t
    |USING (SELECT * from update_table2) as s
    |ON t.id = s.id
    |WHEN MATCHED AND s._op = 'd' THEN DELETE
    |WHEN MATCHED AND s._op = 'u' THEN UPDATE SET *
    |WHEN NOT MATCHED THEN INSERT *
    |""".stripMargin
spark.sql(writeSql)
spark.sql("select * from hive.db.tbl_test").show



import org.apache.iceberg.spark.SparkSchemaUtil;



import org.apache.iceberg.spark.SparkSchemaUtil;

val schema2 = SparkSchemaUtil.schemaForTable(spark, "table_name");
val res = new java.util.ArrayList[Row]()
