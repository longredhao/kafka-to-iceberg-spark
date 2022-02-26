package org.apache.iceberg.streaming.core.broadcast

import org.apache.avro.Schema

import scala.collection.immutable.HashMap

/**
 * Schema 全局广播变量.
 * 用于在更新 SchemaAccumulator 时, 快速在 Schema Value 和 Schema Version 之间进行类型转换
 *
 * @param schemaToVersionMap Avro Schema  到 Schema Register Server 的 Schema Version 的映射
 *
 * @param versionToSchemaMap Schema Register Server 的 Schema Version 到 Avro Schema 的映射
 */
case class SchemaBroadcast(
                            schemaToVersionMap: HashMap[Schema, Integer],
                            versionToSchemaMap: HashMap[Integer, Schema],
                          ) extends Serializable {
  override def toString = s"SchemaBroadcast($schemaToVersionMap, $versionToSchemaMap)"
}
