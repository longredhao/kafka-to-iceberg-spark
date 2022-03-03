// /opt/run/spark3/bin/spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.1


import org.apache.avro.Schema.Parser
import org.apache.iceberg.{Schema, UpdateSchema}
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.{SparkCatalog, SparkSchemaUtil}
import org.apache.iceberg.streaming.config.TableCfg
import org.apache.iceberg.types.Types.NestedField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import java.util
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


val spark = SparkSession.builder().
  master("local[*]").
  config("spark.yarn.jars.partitionOverwriteMode", "hdfs:/user/share/libs/spark/3.2.1/*.jar,hdfs:/user/share/libs/kafka2iceberg/0.1.0/*.jar").
  config("spark.sql.sources.partitionOverwriteMode", "dynamic").
  config("spark.sql.extensions" , "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").
  config("spark.sql.catalog.hadoop" , "org.apache.iceberg.spark.SparkCatalog").
  config("spark.sql.catalog.hadoop.type" , "hadoop").
  config("spark.sql.catalog.hadoop.warehouse", "hdfs://hadoop:8020/user/hadoop/warehouse").
  config("spark.sql.warehouse.dir", "hdfs://hadoop:8020/user/hadoop/warehouse").
  appName("Kafka2Iceberg").getOrCreate()
spark.sql("show databases").show

spark.conf.getAll.foreach(println)

val catalog = new HadoopCatalog();
catalog.setConf(spark.sparkContext.hadoopConfiguration);  // Configure using Spark's Hadoop configuration
val properties = new util.HashMap[String, String]()
properties.put("warehouse", "hdfs://hadoop:8020/user/hadoop/warehouse");
catalog.initialize("hadoop", properties);


val namespace = Namespace.of("db_test")
if (!catalog.namespaceExists(namespace)) catalog.createNamespace(namespace)




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

val schema1 = new Parser().parse(schemaStr1)
val avroSchema1 =  new  org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(schema1.toString())
val icebergSchema1 = AvroSchemaUtil.toIceberg(avroSchema1)

val schema2 = new Parser().parse(schemaStr2)
val avroSchema2 =  new  org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(schema2.toString())
val icebergSchema2 = AvroSchemaUtil.toIceberg(avroSchema2)

val schema3 = new Parser().parse(schemaStr3)
val avroSchema3 =  new  org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(schema3.toString())
val icebergSchema3 = AvroSchemaUtil.toIceberg(avroSchema3)

spark.sql("create table hadoop.db_test.tbl_test")



/* Create Table with Schema 1, and int */
spark.sql("drop table hadoop.db_test.tbl_test")
val icebergTableName = "hadoop.db_test.tbl_test"
val tempTable = s"${icebergTableName}_temp"
val tempTable = s"${icebergTableName.replace(".","_")}_temp"
val mergeSql =
  s"""
     |MERGE INTO hadoop.db_test.tbl_test AS t
     |USING (SELECT * from $tempTable) AS s
     |ON t.id = s.id
     |WHEN MATCHED AND s._op = 'u' THEN UPDATE SET *
     |WHEN MATCHED AND s._op = 'd' THEN DELETE
     |WHEN NOT MATCHED THEN INSERT *
     |""".stripMargin


val tableIdentifier = TableIdentifier.of("db_test", "tbl_test")
val tbl_test = catalog.createTable(tableIdentifier, icebergSchema1)
spark.sql("select * from hadoop.db_test.tbl_test").show

Seq((1, "Zena", 20, "r")).toDF("id", "name", "age", "_op").createOrReplaceTempView(tempTable)
spark.sql(s"select * from $tempTable").show

spark.sql(mergeSql)
spark.sql("select * from hadoop.db_test.tbl_test").show(false)
spark.sql("show tables").show





val icebergTableName = "hadoop.db_test.tbl_test"
val tempTable = s"${icebergTableName.replace(".","_")}_temp"
Seq((1, "Zena", 20, "r")).toDF("id", "name", "age", "_op").createOrReplaceTempView(tempTable)
spark.sql(s"select * from $tempTable").show



(spark.sql.warehouse.dir,file:/opt/spark-warehouse)
(spark.driver.host,hadoop)
(spark.driver.port,41355)
(spark.repl.class.uri,spark://hadoop:41355/classes)
  (spark.jars,file:///root/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-3.2_2.12-0.13.1.jar)
(spark.repl.class.outputDir,/tmp/spark-527348cd-de13-4586-a57c-a793f3b94eab/repl-19c0fe6c-7abc-4d8a-9882-8dacda855b2e)
(spark.app.name,Spark shell)
(spark.submit.pyFiles,)
(spark.ui.showConsoleProgress,true)
(spark.app.startTime,1646283025400)
(spark.executor.id,driver)
(spark.app.initial.jar.urls,spark://hadoop:41355/jars/org.apache.iceberg_iceberg-spark-runtime-3.2_2.12-0.13.1.jar)
  (spark.submit.deployMode,client)
(spark.master,local[*])
(spark.home,/opt/run/spark3)
(spark.sql.catalogImplementation,hive)
(spark.repl.local.jars,file:///root/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-3.2_2.12-0.13.1.jar)
  (spark.app.id,local-1646283025957)



spark.stop()

val spark = SparkSession.builder().master("local[*]").getOrCreate()
  master("local[*]").
  config("spark.yarn.jars.partitionOverwriteMode", "hdfs:/user/share/libs/spark/3.2.1/*.jar,hdfs:/user/share/libs/kafka2iceberg/0.1.0/*.jar").
  config("spark.sql.sources.partitionOverwriteMode", "dynamic").
//  config("spark.sql.extensions" , "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").
//  config("spark.sql.catalog.hadoop" , "org.apache.iceberg.spark.SparkCatalog").
//  config("spark.sql.catalog.hadoop.type" , "hadoop").
//  config("spark.sql.catalog.hadoop.warehouse", "hdfs://hadoop:8020/user/hadoop/warehouse").
  config("spark.sql.warehouse.dir", "file:/opt/spark-warehouse").
  appName("Kafka2Iceberg").getOrCreate()
spark.sql("show databases").show




(spark.sql.catalog.hadoop,org.apache.iceberg.spark.SparkCatalog)
(spark.sql.warehouse.dir,hdfs://hadoop:8020/user/hadoop/warehouse)
  (spark.driver.host,hadoop)
(spark.driver.port,40873)
(spark.jars,file:///root/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-3.2_2.12-0.13.1.jar)
  (spark.yarn.jars.partitionOverwriteMode,hdfs:/user/share/libs/spark/3.2.1/*.jar,hdfs:/user/share/libs/kafka2iceberg/0.1.0/*.jar)
(spark.app.name,Kafka2Iceberg)
(spark.submit.pyFiles,)
(spark.ui.showConsoleProgress,true)
(spark.app.startTime,1646283202251)
(spark.executor.id,driver)
(spark.app.initial.jar.urls,spark://hadoop:40873/jars/org.apache.iceberg_iceberg-spark-runtime-3.2_2.12-0.13.1.jar)
(spark.sql.sources.partitionOverwriteMode,dynamic)
(spark.submit.deployMode,client)
(spark.master,yarn)
(spark.ui.filters,org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter)
(spark.sql.catalogImplementation,hive)
(spark.sql.catalog.hadoop.warehouse,hdfs://hadoop:8020/user/hadoop/warehouse)
(spark.driver.appUIAddress,http://hadoop:4040)
(spark.repl.local.jars,file:///root/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-3.2_2.12-0.13.1.jar)
(spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_HOSTS,hadoop)
(spark.sql.catalog.hadoop.type,hadoop)
(spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES,http://hadoop:8088/proxy/application_1646272753723_0008)
(spark.sql.extensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions)
(spark.app.id,application_1646272753723_0008)
