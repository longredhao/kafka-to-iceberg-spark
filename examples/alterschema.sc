// /opt/run/spark3/bin/spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.1


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
  master("yarn").
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


val catalog = new HiveCatalog();
catalog.setConf(spark.sparkContext.hadoopConfiguration);  // Configure using Spark's Hadoop configuration
val properties = new util.HashMap[String, String]()
properties.put("warehouse", "hdfs://hadoop:8020/user/hive/warehouse/iceberg");
properties.put("uri", "thrift://hadoop:9083");
catalog.initialize("hive", properties);

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

import spark.implicits._
val schema1 = new Parser().parse(schemaStr1)
val avroSchema1 =  new  org.apache.avro.Schema.Parser().parse(schema1.toString())
val icebergSchema1 = AvroSchemaUtil.toIceberg(avroSchema1)

val schema2 = new Parser().parse(schemaStr2)
val avroSchema2 =  new org.apache.avro.Schema.Parser().parse(schema2.toString())
val icebergSchema2 = AvroSchemaUtil.toIceberg(avroSchema2)

val schema3 = new Parser().parse(schemaStr3)
val avroSchema3 =  new org.apache.avro.Schema.Parser().parse(schema3.toString())
val icebergSchema3 = AvroSchemaUtil.toIceberg(avroSchema3)




/* Create Table with Schema 1, and int */
spark.sql("drop table hive.db_gb18030_test.tbl_schema_test")
val icebergTableName = "hive.db_gb18030_test.tbl_schema_test"
val tempTable = s"${icebergTableName.replace(".","_")}_temp"
val mergeSql =
  s"""
     |MERGE INTO hive.db_gb18030_test.tbl_schema_test AS t
     |USING (SELECT * from $tempTable) AS s
     |ON t.id = s.id
     |WHEN MATCHED AND s._op = 'u' THEN UPDATE SET *
     |WHEN MATCHED AND s._op = 'd' THEN DELETE
     |WHEN NOT MATCHED THEN INSERT *
     |""".stripMargin


val tableIdentifier = TableIdentifier.of("db_gb18030_test", "tbl_schema_test");
val tbl_schema_test = catalog.createTable(tableIdentifier, icebergSchema1)
Seq((1, "Zena", 20, "r")).toDF("id", "name", "age", "_op").createOrReplaceTempView(tempTable)
spark.sql(mergeSql)
spark.sql("select * from hive.db_gb18030_test.tbl_schema_test").show(false)
spark.sql("show tables").show

// ========================================================================
/* 添加一列 score */
val catalogTable = catalog.loadTable(tableIdentifier)
catalogTable.updateSchema().unionByNameWith(icebergSchema2).commit()
spark.sql("select * from hive.db_gb18030_test.tbl_schema_test").show(false)
Seq((1, "Zena", 20, 91,"u"),(2, "Area", 23, 95,"c")).toDF("id", "name", "age", "score", "_op").createOrReplaceTempView(tempTable)
spark.sql(mergeSql)
spark.sql("select * from hive.db_gb18030_test.tbl_schema_test").show(false)


/* 删除一列 age */
val catalogTable = catalog.loadTable(tableIdentifier)
catalogTable.updateSchema().unionByNameWith(icebergSchema3).makeColumnOptional("age").commit()
spark.sql("select * from hive.db_gb18030_test.tbl_schema_test").show(false)
Seq((1, "Zena", 93,"u"),(3, "Area", 98,"c")).toDF("id", "name", "score", "_op").createOrReplaceTempView(tempTable)
spark.sql(mergeSql)
spark.sql("select * from hive.db_gb18030_test.tbl_schema_test").show(false)



spark.sql("insert into hive.db_gb18030_test.tbl_schema_test('id','name','score','_op') values(3, 'c', 100, 'u')")
spark.sql("select * from hive.db_gb18030_test.tbl_schema_test").show(false)


def checkAndAlterTableSchema(spark: SparkSession, tableCfg: TableCfg, enableDropColumn: Boolean = false): Unit = {
}



// ====================================== 函数测试 ==================================

spark.sql("drop table hive.db_gb18030_test.tbl_schema_test")
val tbl_schema_test = catalog.createTable(tableIdentifier, icebergSchema1)


val icebergTableName = "hive.db_gb18030_test.tbl_schema_test"
checkAndAlterTableSchema(spark, icebergTableName, avroSchema3, true)

val catalogTable = catalog.loadTable(tableIdentifier)



// ======================================Jar 包 函数测试 ==================================

spark.sql("show tables").show
spark.sql("drop table hive.db_gb18030_test.tbl_test")
spark.sql("show tables").show

val icebergTableName = "hive.db_gb18030_test.tbl_test"
val tableNameItems = icebergTableName.split("\\.", 3)
val tableIdentifier = TableIdentifier.of(tableNameItems(1), tableNameItems(2));
val catalogTable = catalog.loadTable(tableIdentifier)

-----------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+
|_src_name|_src_db        |_src_table|_src_ts_ms   |_src_server_id|_src_file       |_src_pos|_src_op|_src_ts_ms_r |_tsc_id                                 |_tsc_total_order|_tsc_data_collection_order|_kfk_topic                   |_kfk_partition|_kfk_offset|_kfk_timestamp|id |c1 |c2 |c3 |c4 |c5  |create_time  |update_time  |
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+
|test     |db_gb18030_test|tbl_test  |1645690498000|1             |mysql-bin.000010|9164    |c      |1645690498874|e45b718e-906f-11ec-89e3-0242c0a8640a:220|1               |1                         |test.db_gb18030_test.tbl_test|1             |0          |1645690499261 |2  |A1 |A2 |1  |1  |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645895249000|1             |mysql-bin.000014|6745    |u      |1645895249588|e45b718e-906f-11ec-89e3-0242c0a8640a:234|1               |1                         |test.db_gb18030_test.tbl_test|1             |1          |1645895249714 |2  |A1 |A2 |2  |1  |null|1645157193000|1645895249000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  |test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
  +---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+


def checkAndAlterTableSchema(spark: SparkSession,
                             icebergTableName: String,
                             curSchema: org.apache.iceberg.shaded.org.apache.avro.Schema,
                             enableDropColumn: Boolean = false): Unit = {

  val tableItems = icebergTableName.split("\\.",3)
  val catalogName = tableItems(0)

  /* 创建 HiveCatalog 对象 */
  val catalog = new HiveCatalog();
  catalog.setConf(spark.sparkContext.hadoopConfiguration)
  val properties = new util.HashMap[String, String]()
  properties.put("warehouse", spark.conf.get("hive.metastore.warehouse.dir"));
  properties.put("uri", spark.conf.get("spark.hadoop.hive.metastore.uris"));
  catalog.initialize(catalogName, properties);

  /* 读取加载 Catalog Table */
  val tableIdentifier = TableIdentifier.of("db_gb18030_test", "tbl_schema_test");
  val catalogTable = catalog.loadTable(tableIdentifier)
  /* 修复 IcebergSchema 的起始ID (使用 AvroSchemaUtil.toIceberg() 函数产生的Schema 从 0 计数, 而 Iceberg 的 Schema 从 1 开始计数) */
  val curIcebergSchema = copySchemaWithStartId(AvroSchemaUtil.toIceberg(curSchema), startId = 1)

  if(!curIcebergSchema.sameSchema(catalogTable.schema())){
    val perColumns = catalogTable.schema().columns()
    val curColumns = curIcebergSchema.columns()
    val perColumnNames = perColumns.map(column =>column.name()).toList
    val curColumnNames = curColumns.map(column =>column.name()).toList

    /* Step 0 : 创建 UpdateSchema 对象 */
    var updateSchema: UpdateSchema = catalogTable.updateSchema()

    /* Step 1 : 添加列 (使用 unionByNameWith 进行合并) */
    updateSchema = updateSchema.unionByNameWith(curIcebergSchema)

    /* Step 2 : 删除列 （基于列名 drop 被删除的列) */
    val deleteColumnNames = perColumnNames diff curColumnNames
    if(deleteColumnNames.nonEmpty ){
      if(enableDropColumn){
        for (name <- deleteColumnNames){
          updateSchema = updateSchema.deleteColumn(name)
        }
      }else{
        throw new RuntimeException("")
      }
    }

    /* Step 3 : 调整列顺序  */
    for(i <- curColumnNames.indices){
      if(i == 0){
        updateSchema = updateSchema.moveFirst(curColumnNames(i))
      } else{
        updateSchema = updateSchema.moveAfter(curColumnNames(i), curColumnNames(i - 1))
      }
    }

    /* Step 4 : 提交执行 Schema 更新  */
    updateSchema.commit()
  }
}


def copySchemaWithStartId(schema: Schema, startId: Int = 0): Schema = {
  val columns = schema.columns()
  val newColumns = new util.ArrayList[NestedField](columns.size())
  for(i <- 0 until columns.size()){
    val column = columns.get(i)
    newColumns.add(NestedField.optional(startId + i, column.name(), column.`type`(), column.doc()))
  }
 new Schema(newColumns)
}