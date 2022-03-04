package catalog

import org.apache.avro.Schema.Parser
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.streaming.Kafka2Iceberg
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.junit.Test

import java.util

class HadoopCatalogTest  extends org.scalatest.FunSuite with Logging{

  @Test
  def testHadoopCatalog(): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder().
      master("local[3]").
      config("spark.yarn.jars", "hdfs:/user/share/libs/spark/3.2.1/*.jar,hdfs:/user/share/libs/kafka2iceberg/0.1.0/*.jar").
      config("spark.sql.sources.partitionOverwriteMode", "dynamic").
      config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").
      config("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog").
      config("spark.sql.catalog.hadoop.type", "hadoop").
      config("spark.sql.catalog.hadoop.warehouse", "hdfs://hadoop:8020/user/test/iceberg").
      config("spark.sql.warehouse.dir", "hdfs://hadoop:8020/user/hive/warehouse").
      enableHiveSupport().
      appName("Kafka2Iceberg").getOrCreate()
    spark.sql("show databases").show

    spark.conf.getAll.foreach(println)

    val catalog = new HadoopCatalog();
    catalog.setConf(spark.sparkContext.hadoopConfiguration); // Configure using Spark's Hadoop configuration
    val properties = new util.HashMap[String, String]()
    properties.put("warehouse", spark.conf.get("spark.sql.catalog.hadoop.warehouse"));
    catalog.initialize("hadoop", properties);

    /* create iceberg namespace */
    val namespace = Namespace.of("db_test")
    if (!catalog.namespaceExists(namespace)) catalog.createNamespace(namespace)

    val schemaStr1 =
      """
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

    val schemaStr2 =
      """
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
    val schemaStr3 =
      """
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

    val schema1 = new Parser().parse(schemaStr1)
    val avroSchema1 = new org.apache.avro.Schema.Parser().parse(schema1.toString())
    val icebergSchema1 = AvroSchemaUtil.toIceberg(avroSchema1)

    val schema2 = new Parser().parse(schemaStr2)
    val avroSchema2 = new org.apache.avro.Schema.Parser().parse(schema2.toString())
    val icebergSchema2 = AvroSchemaUtil.toIceberg(avroSchema2)

    val schema3 = new Parser().parse(schemaStr3)
    val avroSchema3 = new org.apache.avro.Schema.Parser().parse(schema3.toString())
    val icebergSchema3 = AvroSchemaUtil.toIceberg(avroSchema3)

    val icebergTableName = "hadoop.db_test.tbl_test"
    val tempTable = s"${icebergTableName.replace(".", "_")}_temp"
    val mergeSql =
      s"""
         |MERGE INTO $icebergTableName AS t
         |USING (SELECT * from $tempTable) AS s
         |ON t.id = s.id
         |WHEN MATCHED AND s._op = 'u' THEN UPDATE SET *
         |WHEN MATCHED AND s._op = 'd' THEN DELETE
         |WHEN NOT MATCHED THEN INSERT *
         |""".stripMargin

    logInfo("\n============================ Begin Create Table ====================================")
    val tableIdentifier = TableIdentifier.of("db_test", "tbl_test")
    catalog.dropTable(tableIdentifier,true)
    catalog.createTable(tableIdentifier, icebergSchema1)
    val catalogTable = catalog.loadTable(tableIdentifier)
    spark.sql(s"select * from $icebergTableName").show
    spark.read.format("iceberg").load(icebergTableName).show
    logInfo(catalog.loadTable(tableIdentifier).schemas().toString)

    logInfo("\n============================ End Create Table =====================================")


    logInfo("============================ Begin Init  Table ======================================")
    import spark.implicits._
    Seq((1, "Zena", 20, "r")).toDF("id", "name", "age", "_op").createOrReplaceTempView(tempTable)
    spark.sql(s"select * from $icebergTableName").show
    spark.read.format("iceberg").load(icebergTableName).show
    logInfo(catalog.loadTable(tableIdentifier).schemas().toString)
    logInfo("============================ End Init  Table ========================================")



    logInfo("============================ Begin Alter Table Add Column score ======================================")

    catalogTable.updateSchema().unionByNameWith(icebergSchema2).commit()
    Seq((1, "Zena", 20, 91,"u"),(2, "Area", 23, 95,"c")).toDF("id", "name", "age", "score", "_op").createOrReplaceTempView(tempTable)
    spark.sql(mergeSql)
    spark.sql(s"select * from $icebergTableName").show(false)
    spark.read.format("iceberg").load(icebergTableName).show
    logInfo(catalog.loadTable(tableIdentifier).schemas().toString)
    logInfo("============================ End Alter Table Add Column score ========================================")


    logInfo("============================ Begin Alter Table Move Colum [age] after [id] ======================================")

    catalogTable.updateSchema().moveAfter("age","id").commit()
    Seq((1,  21, "Zena", 91,"u"),(3,  34, "Bre", 98,"c")).toDF("id", "age", "name",  "score", "_op").createOrReplaceTempView(tempTable)
    spark.sql(mergeSql)
    spark.sql(s"select * from $icebergTableName").show(false)
    spark.read.format("iceberg").load(icebergTableName).show
    logInfo(catalog.loadTable(tableIdentifier).schemas().toString)
    logInfo("============================ End Alter Table Move Colum [age] after [id] ========================================")



  }

}
