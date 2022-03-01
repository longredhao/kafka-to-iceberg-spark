
import org.apache.avro.Schema.Parser
import org.apache.iceberg.{Schema, UpdateSchema}
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.streaming.config.TableCfg
import org.apache.iceberg.types.Types.NestedField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import java.util
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


spark.stop()

val spark = SparkSession.builder().
  master("local[2]").
  config("spark.sql.sources.partitionOverwriteMode", "dynamic").
  config("spark.sql.extensions" , "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").
  config("spark.sql.catalog.hive" , "org.apache.iceberg.spark.SparkCatalog").
  config("spark.sql.catalog.hive.type" , "hive").
  config("spark.hadoop.hive.metastore.uris" , "thrift://hadoop:9083").
  config("hive.metastore.warehouse.dir", "hdfs://hadoop:8020/user/hive/warehouse").
  enableHiveSupport().
  appName("Kafka2Iceberg").getOrCreate()
spark.sql("show databases").show

spark.sql("use db_gb18030_test")
spark.sql("show tables").show


spark.sql("drop table hive.db_gb18030_test.tbl_test_1").show



