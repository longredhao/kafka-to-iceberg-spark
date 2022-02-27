// /opt/run/spark3/bin/spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.1


import org.apache.spark.sql.{Row, SparkSession}

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

val createDDL2 =
  """
    |CREATE TABLE hive.db_gb18030_test.tbl_test2 (
    |_src_name string, _src_db string, _src_table string, _src_ts_ms long, _src_server_id long, _src_file string, _src_pos long, _src_op string, _src_ts_ms_r long, _tsc_id string, _tsc_total_order long, _tsc_data_collection_order long, _kfk_topic string, _kfk_partition int, _kfk_offset long, _kfk_timestamp long, ID int, C1 string, C2 string, C3 int, C4 long, CREATE_TIME long, UPDATE_TIME long)
    |USING iceberg
    |""".stripMargin
spark.sql(createDDL2)
spark.sql("show tables").show
spark.sql("select * from  hive.db_gb18030_test.tbl_test").show


val createDDL1 =
  """
    |CREATE TABLE hive.db_gb18030_test.tbl_test1 (
    |_src_name string, _src_db string, _src_table string, _src_ts_ms long, _src_server_id long, _src_file string, _src_pos long, _src_op string, _src_ts_ms_r long, _tsc_id string, _tsc_total_order long, _tsc_data_collection_order long, _kfk_topic string, _kfk_partition int, _kfk_offset long, _kfk_timestamp long, ID int, C1 string, C2 string, C3 int, C4 long, CREATE_TIME long, UPDATE_TIME long)
    |USING iceberg
    |PARTITIONED BY (C1)
    |LOCATION 'hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test1'
    |""".stripMargin
spark.sql(createDDL1)
spark.sql("show tables").show
spark.sql("select * from  hive.db_gb18030_test.tbl_test1").show


val createDDL2 =
  """
    |CREATE TABLE hive.db_gb18030_test.tbl_test2 (
    |_src_name string, _src_db string, _src_table string, _src_ts_ms long, _src_server_id long, _src_file string, _src_pos long, _src_op string, _src_ts_ms_r long, _tsc_id string, _tsc_total_order long, _tsc_data_collection_order long, _kfk_topic string, _kfk_partition int, _kfk_offset long, _kfk_timestamp long, ID int, C1 string, C2 string, C3 int, C4 long, CREATE_TIME long, UPDATE_TIME long)
    |USING iceberg
    |PARTITIONED BY (C1)
    |LOCATION 'hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test2'
    |COMMENT 'db_gb18030_test tbl_test2'
    |TBLPROPERTIES ('read.split.target-size'='268435456')
    |""".stripMargin
spark.sql(createDDL2)
spark.sql("show tables").show
spark.sql("select * from  hive.db_gb18030_test.tbl_test2").show


val createDDL24 =
  """
    |CREATE TABLE hive.db_gb18030_test.tbl_test (
    |ID int, C1 string, C2 string, C3 int, C4 long, CREATE_TIME long, UPDATE_TIME long)
    |USING iceberg
    |PARTITIONED BY (C1)
    |LOCATION 'hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test'
    |COMMENT 'db_gb18030_test tbl_test'
    |TBLPROPERTIES ('read.split.target-size'='268435456')
    |""".stripMargin




//---------

val createDDL2 =
  """
    |CREATE TABLE hive.db_gb18030_test.tbl_test (
    |ID int, C1 string, C2 string, C3 int, C4 long, CREATE_TIME long, UPDATE_TIME long)
    |USING iceberg
    |PARTITIONED BY (C1)
    |LOCATION 'hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test'
    |COMMENT 'db_gb18030_test tbl_test'
    |TBLPROPERTIES ('read.split.target-size'='268435456')
    |""".stripMargin

spark.sql(createDDL2)

spark.sql("show tables").show









val createDDL2 =
  """
    |CREATE TABLE hive.db_gb18030_test.tbl_test (
    |ID int, C1 string, C2 string, C3 int, C4 long, CREATE_TIME long, UPDATE_TIME long)
    |USING iceberg
    |PARTITIONED BY (C1)
    |LOCATION 'hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test'
    |COMMENT 'db_gb18030_test tbl_test'
    |TBLPROPERTIES ('read.split.target-size'='268435456')
    |""".stripMargin
