package org.apache.iceberg.streaming.avro.schema.broadcast

import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType

import scala.collection.immutable.HashMap

/**
 * Schema 全局广播变量.
 * @param schemaToVersionMap Avro Schema String 到 Schema Register Server 的 Schema Version 的映射
 *                           用于在更新 SchemaAccumulator 时快速根据 Schema 获取 Schema Version 值
 * @param versionToSchemaMap Schema Register Server 的 Schema Version 到 Avro Schema String 的映射
 */
case class SchemaBroadcast(
                            schemaToVersionMap: HashMap[String, Integer],
                            versionToSchemaMap: HashMap[Integer, String],
                     ) extends Serializable {


}

