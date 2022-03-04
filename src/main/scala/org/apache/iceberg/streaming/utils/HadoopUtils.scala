package org.apache.iceberg.streaming.utils

import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Types.NestedField
import org.apache.iceberg.{Schema, UpdateSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


/**
 *  Hadoop Catalog 数据处理工具类.
 */
object HadoopUtils extends Logging{

  /**
   *
   * 返回值：
   *    True: 表不存在
   */

  /**
   * 创建 HadoopCatalog Table.
   * 根据返回值经判断是否需要建立 Hive External Table
   * @param spark  SparkSession
   * @param icebergTableName  icebergTableName
   * @param structType Spark DataFrame Schema StructType
   * @param partitionBy partitionBy
   * @param comment comment
   * @param tblProperties tblProperties
   * @return True(表不存在,创建 HadoopCatalog Table), False(表已存在)
   */
  def createHadoopTableIfNotExists(spark: SparkSession,
                                   icebergTableName: String,
                                   structType: StructType,
                                   partitionBy: String,
                                   comment: String,
                                   tblProperties: String
                                  ): Boolean = {
    val tableItems = icebergTableName.split("\\.", 3)
    val namespace = tableItems(1)
    val tableName = tableItems(2)

    /* 创建 HadoopCatalog 对象 */
    val hadoopCatalog = new HadoopCatalog()
    hadoopCatalog.setConf(spark.sparkContext.hadoopConfiguration); // Configure using Spark's Hadoop configuration
    val properties = new util.HashMap[String, String]()
    properties.put("warehouse", spark.conf.get("spark.sql.catalog.hadoop.warehouse"))
    hadoopCatalog.initialize("hadoop", properties)

    /* 如果表不存在则创建表 */
    val tableIdentifier = TableIdentifier.of(namespace, tableName)
    if(!hadoopCatalog.tableExists(tableIdentifier)){
      /* 如果 namespace 不存在则创建  namespace  */
      if (!hadoopCatalog.namespaceExists(Namespace.of(namespace))){
        logInfo(s"Hadoop catalog [$hadoopCatalog], namespace [$namespace] not exists, try to create namespace")
        hadoopCatalog.createNamespace(Namespace.of(namespace))
      }else{
        logInfo(s"Hadoop catalog [$hadoopCatalog], namespace [$namespace] exists")
      }
      val icebergSchema = SparkSchemaUtil.convert(structType)
      val columnList =  icebergSchema.columns().map(c => s"${c.name()} ${c.`type`()}").mkString(", ")

      val createDDL = s"""
                         |CREATE TABLE $icebergTableName (
                         |$columnList)
                         |USING iceberg
                         |PARTITIONED BY ($partitionBy)
                         |COMMENT '$comment'
                         |TBLPROPERTIES ($tblProperties)
                         |""".stripMargin

      logInfo(s"Hadoop table not exists, start create namespace[$namespace]-table[$icebergTableName] with sql [$createDDL]")
      spark.sql(createDDL)
      hadoopCatalog.close()
      true
    }else{
      logInfo("Hadoop table is early exist, ignore create table " )
      false
    }
  }



  /**
   * 使用 检测表结构 更新 - 如果 mergeFlag 为 true 则将表结构更新为合并后的表结构
   * @param spark SparkSession
   * @param icebergTableName iceberg table name
   * @param curSchema Avro Schema
   * @param enableDropColumn  如果为 true 则 Drop 删除的列， 否则且抛出异常
   * @return 表结构更新检测结构,用于返回判断是否需要更新重建 Hive 表: false-Schema未更新, true-Schema发生了更新
   *
   */
  def checkAndAlterHadoopTableSchema(spark: SparkSession,
                                     icebergTableName: String,
                                     curSchema: org.apache.avro.Schema,
                                     enableDropColumn: Boolean = false): Boolean = {

    val tableItems = icebergTableName.split("\\.",3)
    val namespace = tableItems(1)
    val tableName = tableItems(2)

    /* 创建 HadoopCatalog 对象 */
    val catalog = new HadoopCatalog()
    catalog.setConf(spark.sparkContext.hadoopConfiguration)
    val properties = new util.HashMap[String, String]()
    properties.put("warehouse", spark.conf.get("spark.sql.catalog.hadoop.warehouse"))
    catalog.initialize("hadoop", properties)

    /* 读取加载 Catalog Table */
    val tableIdentifier = TableIdentifier.of(namespace, tableName)
    val catalogTable = catalog.loadTable(tableIdentifier)
    /* 修复 IcebergSchema 的起始ID (使用 AvroSchemaUtil.toIceberg() 函数产生的Schema 从 0 计数, 而 Iceberg 的 Schema 从 1 开始计数) */
    val curIcebergSchema = copySchemaWithStartId(AvroSchemaUtil.toIceberg(curSchema), startId = 1, lowerCase = true)
    val catalogTableSchema =  copyFilterMetadataColumns(catalogTable.schema(), 1, lowerCase = true)

    if(!curIcebergSchema.sameSchema(catalogTableSchema)){
      logInfo(s"Table [$icebergTableName] schema changed, before [${catalogTable.schema().toString}]")
      val perColumns = catalogTable.schema().columns()
      val curColumns = curIcebergSchema.columns().map(c => NestedField.optional(c.fieldId(), c.name().toLowerCase(), c.`type`(), c.doc()))
      val perColumnNames = perColumns.map(column =>column.name()).toList
      val curColumnNames = curColumns.map(column =>column.name()).toList

      /* Step 0 : 创建 UpdateSchema 对象 */
      var updateSchema: UpdateSchema = catalogTable.updateSchema()

      /* Step 1 : 添加列 (使用 unionByNameWith 进行合并) */
      updateSchema = updateSchema.unionByNameWith(curIcebergSchema)

      /* Step 2 : 删除列 （基于列名 drop 被删除的列), filter 过滤掉 定义的 metadata 列（以 _ 开头） */
      val deleteColumnNames = perColumnNames.diff(curColumnNames).filter(!_.startsWith("_"))
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
      val  lastMetadataColumn =  perColumnNames.filter(_.startsWith("_")).last
      for(i <- curColumnNames.indices){
        if(i == 0){
          updateSchema = updateSchema.moveAfter(curColumnNames(i), lastMetadataColumn)
        } else{
          updateSchema = updateSchema.moveAfter(curColumnNames(i), curColumnNames(i-1))
        }
      }

      /* Step 3 : 提交执行 Schema 更新  */
      logInfo(s"Try to alter table to ${updateSchema.apply().toString}")
      updateSchema.commit()
      logInfo(s"Table [$icebergTableName] schema changed success ")
      catalog.close()
       true
    }else{
      logInfo(s"Table [$icebergTableName] schema changed did not changed")
       false
    }
  }


  /**
   * Cupy 修改 Iceberg Schema 的起始ID.
   * (使用 AvroSchemaUtil.toIceberg() 函数产生的Schema 从 0 计数, 而 Iceberg 的 Schema 从 1 开始计数
   * @param schema Schema
   * @param startId start index id
   * @param lowerCase column past to lowerCase 列名是否小写
   * @return Schema
   */
  def copySchemaWithStartId(schema: Schema, startId: Int = 0, lowerCase: Boolean): Schema = {
    val columns = schema.columns()
    val newColumns = new util.ArrayList[NestedField](columns.size())
    for(i <- 0 until columns.size()){
      val column = columns.get(i)
      if(lowerCase){
        newColumns.add(NestedField.optional(startId + i, column.name().toLowerCase, column.`type`(), column.doc()))
      }else{
        newColumns.add(NestedField.optional(startId + i, column.name(), column.`type`(), column.doc()))
      }
    }
    new Schema(newColumns)
  }

  /**
   * Cupy 过滤 Iceberg Schema 的 Metadata 列信息. 用于与当前的 Schema 进行对比判断，是否需要更新表结构.
   * @param schema Schema
   * @param startId 结构 Schema 的开始 id
   * @param lowerCase 列名是否小写
   * @return Schema
   */
  def copyFilterMetadataColumns(schema: Schema, startId: Int = 0, lowerCase: Boolean): Schema = {
    val columns = schema.columns()
    val newColumns = new util.ArrayList[NestedField](columns.size())
    val firstIndex = columns.count(_.name().startsWith("_"))  /* 第一个 非 Metadata 的列 */
    for(i <- firstIndex until columns.size()){
      val column = columns.get(i)
      if(lowerCase){
        newColumns.add(NestedField.optional(i - firstIndex + startId, column.name().toLowerCase, column.`type`(), column.doc()))
      }else{
        newColumns.add(NestedField.optional(i - firstIndex + startId , column.name(), column.`type`(), column.doc()))
      }
    }
    new Schema(newColumns)
  }




  //  /**
  //   * 基于 Spark StructType 创建 Iceberg Table 包含 [HadoopCatalog Table] 和 [HiveCatalog Table]
  //   * @param spark  SparkSession
  //   * @param structType  StructType
  //   * @param tableCfg TableCfg
  //   * @return Create Status
  //   */
  //  def createTableIfNotExists(spark: SparkSession,
  //                             tableCfg: TableCfg,
  //                             structType: StructType
  //                            ): Unit = {
  //    val cfg = tableCfg.getCfgAsProperties
  //    val icebergTableName = cfg.getProperty(RunCfg.ICEBERG_TABLE_NAME)
  //    val tableItems = icebergTableName.split("\\.", 3)
  //    val namespace = tableItems(1)
  //    val tableName = tableItems(2)
  //    val partitionBy = cfg.getProperty(RunCfg.ICEBERG_TABLE_PARTITION_BY)
  //    val warehouse = cfg.getProperty(RunCfg.SPARK_SQL_CATALOG_HADOOP_WAREHOUSE)
  //    val location = warehouse + "/" + namespace + "/" + tableName
  //    val comment = cfg.getProperty(RunCfg.ICEBERG_TABLE_COMMENT)
  //    val tblProperties = cfg.getProperty(RunCfg.ICEBERG_TABLE_PROPERTIES)
  //
  //
  //    createHadoopTableIfNotExists(spark,icebergTableName, structType, partitionBy, location, comment, tblProperties)
  //
  //  }

  //  /**
  //   * 创建 HadoopCatalog Table.
  //   */
  //  def createHiveTableIfNotExists(spark: SparkSession,
  //                                   icebergTableName: String,
  //                                   structType: StructType,
  //                                   partitionBy: String,
  //                                   location: String,
  //                                   comment: String,
  //                                   tblProperties: String
  //                                  ): Unit = {
  //    val tableItems = icebergTableName.split("\\.", 3)
  //    val namespace = tableItems(1)
  //    val tableName = tableItems(2)
  //
  //    /* 创建 HiveCatalog 对象 */
  //    val hiveCatalog = new HiveCatalog();
  //    hiveCatalog.setConf(spark.sparkContext.hadoopConfiguration); // Configure using Spark's Hadoop configuration
  //    val properties = new util.HashMap[String, String]()
  //    properties.put("warehouse", spark.conf.get("spark.sql.warehouse.dir"))
  //    properties.put("uri", spark.conf.get("spark.hadoop.hive.metastore.uris"))
  //    hiveCatalog.initialize("hive", properties);
  //
  //
  //    /* 如果表不存在则创建表 */
  //    val tableIdentifier = TableIdentifier.of(namespace, tableName)
  //    if(!hiveCatalog.tableExists(tableIdentifier)){
  //      /* 如果 namespace 不存在则创建  namespace  */
  //      if (!hiveCatalog.namespaceExists(Namespace.of(namespace))){
  //        logInfo(s"Hive catalog [$hiveCatalog], namespace [$namespace] not exists, try to create namespace")
  //        hiveCatalog.createNamespace(Namespace.of(namespace))
  //      }else{
  //        logInfo(s"Hive catalog [$hiveCatalog], namespace [$namespace] exists")
  //      }
  //      val icebergSchema = SparkSchemaUtil.convert(structType)
  //      val columnList =  icebergSchema.columns().map(c => s"${c.name()} ${c.`type`()}").mkString(", ")
  //      val createDDL =  getCreateExternalHiveTableDdl(columnList, icebergTableName,
  //        partitionBy, location, comment, tblProperties)
  //
  //      logInfo(s"Hive table not exists, start create namespace[$namespace]-table[$icebergTableName] with sql [$createDDL]")
  //      spark.sql(createDDL)
  //    }else{
  //      logInfo("Hive table is early exist, ignore create table " )
  //    }
  //
  //  }











  //  /**
  //   * 基于 Spark StructType 生成创建 Iceberg 表的 Create Table DDL SQL
  //   * @param tableCfg TableCfg
  //   * @param sparkType  StructType
  //   * @return
  //   */
  //  def getCreateDdlByStructType(tableCfg: TableCfg, sparkType: StructType): String = {
  //    val cfg = tableCfg.getCfgAsProperties
  //    val icebergTableName = cfg.getProperty(RunCfg.ICEBERG_TABLE_NAME)
  //    val tableItems = icebergTableName.split("\\.", 3)
  //    val namespace = tableItems(1)
  //    val tableName = tableItems(2)
  //    val partitionBy = cfg.getProperty(RunCfg.ICEBERG_TABLE_PARTITION_BY)
  //    val warehouse = cfg.getProperty(RunCfg.SPARK_SQL_CATALOG_HADOOP_WAREHOUSE)
  //    val location = warehouse + "/" + namespace + "/" + tableName
  //    val comment = cfg.getProperty(RunCfg.ICEBERG_TABLE_COMMENT)
  //    val tblProperties = cfg.getProperty(RunCfg.ICEBERG_TABLE_PROPERTIES)
  //    val icebergSchema  =  SparkSchemaUtil.convert(sparkType)
  //    val columnList =  icebergSchema.columns().map(c => s"${c.name()} ${c.`type`()}").mkString(", ")
  //    getCreateHadoopTableDdl(columnList, icebergTableName, partitionBy, location, comment, tblProperties)
  //  }



  //
  //  /**
  //   * 通过 Avro Schema 生成 Create Table DDL SQL
  //   * @param tableCfg TableCfg
  //   * @param schema  Avro Schema
  //   */
  //  @deprecated
  //  def getCreateDdlBySchema(tableCfg: TableCfg, schema: Schema): String = {
  //    val cfg = tableCfg.getCfgAsProperties
  //    val icebergTableName = cfg.getProperty(RunCfg.ICEBERG_TABLE_NAME)
  //    val partitionBy = cfg.getProperty(RunCfg.ICEBERG_TABLE_PARTITION_BY)
  //    val location = cfg.getProperty(RunCfg.ICEBERG_TABLE_LOCATION)
  //    val comment = cfg.getProperty(RunCfg.ICEBERG_TABLE_COMMENT)
  //    val tblProperties = cfg.getProperty(RunCfg.ICEBERG_TABLE_PROPERTIES)
  //    getCreateDdlBySchema(schema, icebergTableName, partitionBy, location, comment, tblProperties)
  //  }
  //
  //
  //  /**
  //   * 通过 Avro Schema 生成 Create Table DDL SQL
  //   * @param schema  Avro Schema
  //   * @param icebergTableName ICEBERG_TABLE_NAME
  //   * @param partitionBy  ICEBERG_TABLE_PARTITION_BY
  //   * @param location  ICEBERG_TABLE_LOCATION
  //   * @param comment ICEBERG_TABLE_COMMENT
  //   * @param tblProperties ICEBERG_TABLE_PROPERTIES
  //   * @return
  //   */
  //  @deprecated()
  //  def getCreateDdlBySchema(schema: org.apache.avro.Schema,
  //                           icebergTableName: String,
  //                           partitionBy: String,
  //                           location: String,
  //                           comment: String,
  //                           tblProperties: String
  //                          ): String = {
  //    val icebergSchema = AvroSchemaUtil.toIceberg(schema)
  //    val columnArr =  icebergSchema.columns().get( schema.getField("after").pos()).`type`().asStructType().fields()
  //      .map(c => s"${c.name()} ${c.`type`()}")
  //
  //    getCreateDllSql(icebergTableName, columnArr.mkString(", "), partitionBy, location, comment, tblProperties)
  //  }
  //
  //  /**
  //   * 基于 Avro Schema 创建 Iceberg Table
  //   * @param spark  SparkSession
  //   * @param schema  Avro Schema
  //   * @param tableCfg TableCfg
  //   * @param dropExist Drop if Exist
  //   * @return Create Status
  //   */
  //  @deprecated
  //  def createTableIfNotExists(spark: SparkSession,
  //                             tableCfg: TableCfg,
  //                             schema: Schema,
  //                             dropExist: Boolean = false): Unit = {
  //    val icebergTableName = tableCfg.getCfgAsProperties.getProperty(RunCfg.ICEBERG_TABLE_NAME)
  //    val items = icebergTableName.split("\\.", 3)
  //    val namespace = items(1)
  //
  //    /* 检测 database/namespace 是否存在, 如果不存在则创建 */
  //    spark.sql(s"create namespace if not exists $namespace").count()
  //
  //    /* 如果表存在需先 drop table */
  //    if(checkTableExists(spark, icebergTableName)){
  //      if(dropExist){
  //        dropTable(spark, icebergTableName)
  //        logInfo(s"check table [$icebergTableName] exist, need drop first")
  //      }else{
  //        logInfo(s"iceberg table [$icebergTableName] exist")
  //        return
  //      }
  //    }
  //    logInfo(s"iceberg table [$icebergTableName] not exist")
  //    spark.sql(s"use $namespace").count()
  //    val createDDL = getCreateDdlBySchema(tableCfg, schema)
  //    logInfo(s"start create table [$icebergTableName] with sql [\n$createDDL\n]")
  //    spark.sql(createDDL).count
  //    checkTableExists(spark, icebergTableName)
  //    logInfo(s"table [$icebergTableName] create success")
  //  }
  //
  //  /**
  //   * Drop Exist Iceberg Table
  //   * @param spark SparkSession
  //   * @param icebergTableName Iceberg Table Name
  //   * @return
  //   */
  //  @deprecated
  //  def dropTable(spark: SparkSession, icebergTableName: String): Boolean ={
  //    val items = icebergTableName.split("\\.", 3)
  //    val namespace = items(1)
  //    val tableName = items(2)
  //    spark.sql(s"use $namespace").count()
  //    spark.sql(s"drop table $icebergTableName").count()
  //    spark.sql("show tables").
  //      where(s"namespace='$namespace' and tableName ='$tableName'").count() == 0
  //  }
  //
  //
  //  /**
  //   * 检测 iceberg catalog database/namespace 是否存在
  //   * @param spark  SparkSession
  //   * @param icebergTableName  iceberg table name
  //   * @return check result
  //   */
  //    @deprecated
  //  def checkNamespaceExists(spark: SparkSession, icebergTableName: String): Boolean = {
  //    val items = icebergTableName.split("\\.", 3)
  //    val namespace = items(1)
  //    spark.sql(s"show namespaces").where(s"namespace='$namespace'").count() == 1
  //  }
  //
  //  /**
  //   * 检测 iceberg table 是否存在
  //   * @param spark  SparkSession
  //   * @param icebergTableName iceberg table name
  //   * @return check result
  //   */
  //    @deprecated
  //  def checkTableExists(spark: SparkSession, icebergTableName: String): Boolean = {
  //    val items = icebergTableName.split("\\.", 3)
  //    val namespace = items(1)
  //    val tableName = items(2)
  //    spark.sql(s"use $namespace")
  //    spark.sql("show tables").
  //      where(s"namespace='$namespace' and tableName ='$tableName'").count() == 1
  //  }
  //
  //  /**
  //   * 创建 HiveCatalog Namespace
  //   * @param spark SparkSession
  //   * @param icebergTableName iceberg table name
  //   */
  //  def createNamespaceIfNotExists(spark: SparkSession, icebergTableName: String): Unit = {
  //    val items = icebergTableName.split("\\.", 3)
  //    val namespace = items(1)
  //    spark.sql(s"create namespace if not exists $namespace").count()
  //  }


}
