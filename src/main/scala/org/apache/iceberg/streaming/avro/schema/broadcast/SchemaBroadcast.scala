package org.apache.iceberg.streaming.avro.schema.broadcast

import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType

import scala.collection.immutable.HashMap

/**
 * Schema 全局广播变量.
 * @param schemaToVersionMap Avro Schema String 到 Schema Register Server 的 Schema Version 的映射
 *                           用于在更新 SchemaAccumulator 时快速根据 Schema 获取 Schema Version 值
 * @param versionToSchemaMap Schema Register Server 的 Schema Version 到 Avro Schema String 的映射
 * @param mergedSchemaStr  历史所有的 Schema 版本的 Merged 结果信息, 由于 Avro 1.8.0 版本的 Schema 类不支持序列化因此使用 String 类型
 * @param mergedStruct  历史所有的 Schema 版本的信息（用于在创建 Dataframe 时追加被删除的列信息）
 */
case class SchemaBroadcast(
                            schemaToVersionMap: HashMap[String, Integer],
                            versionToSchemaMap: HashMap[Integer, String],
                            mergedSchemaStr: String,
                            mergedStruct: StructType
                     ) extends Serializable {


  /**
   * 获取历史所有的 Schema 版本的 Merged 结果信息
   * @return Schema
   */
  def getMergedSchema: Schema = {
    new Schema.Parser().parse(mergedSchemaStr)
  }




}

