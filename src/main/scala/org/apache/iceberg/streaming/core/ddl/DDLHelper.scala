package org.apache.iceberg.streaming.core.ddl

import org.apache.avro.Schema
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.streaming.config.{RunCfg, TableCfg}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * DDL 工具类
 */
class DDLHelper{

}

object DDLHelper extends Logging{


  /**
   * 基于 Spark StructType 创建 Iceberg Table
   * @param spark  SparkSession
   * @param structType  StructType
   * @param tableCfg TableCfg
   * @return Create Status
   */
  def createTableIfNotExists(spark: SparkSession,
                             tableCfg: TableCfg,
                             structType: StructType
                            ): Unit = {
    val icebergTableName = tableCfg.getCfgAsProperties.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val items = icebergTableName.split("\\.", 3)
    val namespace = items(1)
    val tableName = items(2)
    spark.sql(s"use $namespace")
    val tableExist = spark.sql("show tables").where(s"namespace='$namespace' and tableName ='$tableName'").count() == 1
    if(!tableExist){
      val createDDL =  getCreateDdlByStructType(tableCfg, structType)
      logInfo(s"start create namespace[$namespace]-table[$icebergTableName] with sql [$createDDL]")
      spark.sql(createDDL)
    }
  }



  /**
   * 检测 iceberg catalog database/namespace 是否存在
   * @param spark  SparkSession
   * @param icebergTableName  iceberg table name
   * @return check result
   */
  def checkNamespaceExists(spark: SparkSession, icebergTableName: String): Boolean = {
    val items = icebergTableName.split("\\.", 3)
    val namespace = items(1)
    spark.sql(s"show namespaces").where(s"namespace='$namespace'").count() == 1
  }


  /**
   * 创建 HiveCatalog Namespace
   * @param spark SparkSession
   * @param icebergTableName iceberg table name
   */
  def createNamespaceIfNotExists(spark: SparkSession, icebergTableName: String): Unit = {
    val items = icebergTableName.split("\\.", 3)
    val namespace = items(1)
    spark.sql(s"create namespace if not exists $namespace").count()
  }




  /**
   * 检测 iceberg table 是否存在
   * @param spark  SparkSession
   * @param icebergTableName iceberg table name
   * @return check result
   */
  def checkTableExists(spark: SparkSession, icebergTableName: String): Boolean = {
    val items = icebergTableName.split("\\.", 3)
    val namespace = items(1)
    val tableName = items(2)
    spark.sql(s"use $namespace")
    spark.sql("show tables").
      where(s"namespace='$namespace' and tableName ='$tableName'").count() == 1
  }

  /**
   * 基于 Spark StructType 生成创建 Iceberg 表的 Create Table DDL SQL
   * @param tableCfg TableCfg
   * @param sparkType  StructType
   * @return
   */
  def getCreateDdlByStructType(tableCfg: TableCfg, sparkType: StructType): String = {
    val cfg = tableCfg.getCfgAsProperties

    val icebergTableName = cfg.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val partitionBy = cfg.getProperty(RunCfg.ICEBERG_TABLE_PARTITION_BY)
    val location = cfg.getProperty(RunCfg.ICEBERG_TABLE_LOCATION)
    val comment = cfg.getProperty(RunCfg.ICEBERG_TABLE_COMMENT)
    val tblProperties = cfg.getProperty(RunCfg.ICEBERG_TABLE_PROPERTIES)

    val icebergSchema  =  SparkSchemaUtil.convert(sparkType)
    getCreateDdlBySchema(icebergSchema, icebergTableName, partitionBy, location, comment, tblProperties)
  }








  /**
   * Drop Exist Iceberg Table
   * @param spark SparkSession
   * @param icebergTableName Iceberg Table Name
   * @return
   */
  def dropTable(spark: SparkSession, icebergTableName: String): Boolean ={
    val items = icebergTableName.split("\\.", 3)
    val namespace = items(1)
    val tableName = items(2)
    spark.sql(s"use $namespace").count()
    spark.sql(s"drop table $icebergTableName").count()
    spark.sql("show tables").
      where(s"namespace='$namespace' and tableName ='$tableName'").count() == 0
  }

  /**
   * 通过 Avro Schema 生成 Create Table DDL SQL
   * @param tableCfg TableCfg
   * @param schema  Avro Schema
   */
  def getCreateDdlBySchema(tableCfg: TableCfg, schema: Schema): String = {
    val cfg = tableCfg.getCfgAsProperties
    val icebergTableName = cfg.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val partitionBy = cfg.getProperty(RunCfg.ICEBERG_TABLE_PARTITION_BY)
    val location = cfg.getProperty(RunCfg.ICEBERG_TABLE_LOCATION)
    val comment = cfg.getProperty(RunCfg.ICEBERG_TABLE_COMMENT)
    val tblProperties = cfg.getProperty(RunCfg.ICEBERG_TABLE_PROPERTIES)
    getCreateDdlBySchema(schema, icebergTableName, partitionBy, location, comment, tblProperties)
  }


  /**
   * 通过 Avro Schema 生成 Create Table DDL SQL
   * @param schema  Avro Schema
   * @param tableName ICEBERG_TABLE_NAME
   * @param partitionBy  ICEBERG_TABLE_PARTITION_BY
   * @param location  ICEBERG_TABLE_LOCATION
   * @param comment ICEBERG_TABLE_COMMENT
   * @param tblProperties ICEBERG_TABLE_PROPERTIES
   * @return
   */
  def getCreateDdlBySchema(schema: Schema,
                           icebergTableName: String,
                           partitionBy: String,
                           location: String,
                           comment: String,
                           tblProperties: String
                          ): String = {
    val icebergSchema = AvroSchemaUtil.toIceberg(schema)
    val columnArr =  icebergSchema.columns().get( schema.getField("after").pos()).`type`().asStructType().fields()
      .map(c => s"${c.name()} ${c.`type`()}")

    getCreateDllSql(icebergTableName, columnArr.mkString(", "), partitionBy, location, comment, tblProperties)
  }

  /**
   * 通过 Iceberg Schema 生成 Create Table DDL SQL
   * @param icebergSchema  Iceberg Schema
   * @param icebergTableName ICEBERG_TABLE_NAME
   * @param partitionBy  ICEBERG_TABLE_PARTITION_BY
   * @param location  ICEBERG_TABLE_LOCATION
   * @param comment ICEBERG_TABLE_COMMENT
   * @param tblProperties ICEBERG_TABLE_PROPERTIES
   * @return
   */
  def getCreateDdlBySchema(icebergSchema: org.apache.iceberg.Schema,
                           icebergTableName: String,
                           partitionBy: String,
                           location: String,
                           comment: String,
                           tblProperties: String
                          ): String = {
    val columnArr =  icebergSchema.columns().map(c => s"${c.name()} ${c.`type`()}")
    getCreateDllSql(icebergTableName, columnArr.mkString(", "), partitionBy, location, comment, tblProperties)
  }



  /**
   * 填充 Create DDL SQL
   * @param icebergTableName  iceberg table name
   * @param columnList 表列名,逗号分隔,: 如： c1 int, c2 string
   * @param partitionBy 分区字段
   * @param location 存储地址
   * @param comment 注释
   * @param tblProperties 表属性
   * @return DDL SQL
   */
  def getCreateDllSql(icebergTableName: String,
                      columnList: String,
                      partitionBy: String,
                      location: String,
                      comment: String,
                      tblProperties: String): String = {
    s"""
       |CREATE TABLE $icebergTableName (
       |$columnList)
       |USING iceberg
       |PARTITIONED BY ($partitionBy)
       |LOCATION '$location'
       |COMMENT '$comment'
       |TBLPROPERTIES ($tblProperties)
       |""".stripMargin
  }



  /**
   * 基于 Avro Schema 创建 Iceberg Table
   * @param spark  SparkSession
   * @param schema  Avro Schema
   * @param tableCfg TableCfg
   * @param dropExist Drop if Exist
   * @return Create Status
   */
  def createTableIfNotExists(spark: SparkSession,
                             tableCfg: TableCfg,
                             schema: Schema,
                             dropExist: Boolean = false): Unit = {
    val icebergTableName = tableCfg.getCfgAsProperties.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val items = icebergTableName.split("\\.", 3)
    val namespace = items(1)

    /* 检测 database/namespace 是否存在, 如果不存在则创建 */
    spark.sql(s"create namespace if not exists $namespace").count()

    /* 如果表存在需先 drop table */
    if(checkTableExists(spark, icebergTableName)){
      if(dropExist){
        dropTable(spark, icebergTableName)
        logInfo(s"check table [$icebergTableName] exist, need drop first")
      }else{
        logInfo(s"iceberg table [$icebergTableName] exist")
        return
      }
    }
    logInfo(s"iceberg table [$icebergTableName] not exist")
    spark.sql(s"use $namespace").count()
    val createDDL = getCreateDdlBySchema(tableCfg, schema)
    logInfo(s"start create table [$icebergTableName] with sql [\n$createDDL\n]")
    spark.sql(createDDL).count
    checkTableExists(spark, icebergTableName)
    logInfo(s"table [$icebergTableName] create success")
  }
}
