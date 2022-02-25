
import java.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser


object GetAlterTableSqlApp {
  def main(args: Array[String]): Unit = {
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
                      |    {"name": "score", "type": ["null", "int"], "default": null},
                      |    {"name": "id", "type": ["null", "int"], "default": null},
                      |    {"name": "pid", "type": ["null", "int"], "default": null},
                      |    {"name": "name", "type": ["null", "string"], "default": null},
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

    getAlterTableSql(schemaStr1,schemaStr2)
    //        getAlterTableSql(schemaStr2,schemaStr3)

    def getAlterTableSql(schemaStr1: String, schemaStr2:String) = {
      val schema1 = new Parser().parse(schemaStr1)
      val schema2 = new Parser().parse(schemaStr2)

      val fields1 = schema1.getFields
      val fields2 = schema2.getFields

      val fieldStr1 = " " + fields1.toString.split('[')(1)
      val fieldStr2 = " " + fields2.toString.split('[')(1)
      val namespace = schema1.getNamespace

      var x=1;
      //增加字段，旧的schema对新的schema（遍历新的schema的字段名）做包含判断，不包含则为新增字段
      for (x <- 0 to fields2.size()-1 ) {
        if(x==0){
          if (!fieldStr1.contains(fields2.get(x).name() + " ")) {
            var fieldName2 = fields2.get(x).name
            var splits = fields2.get(x).schema().toString().split('"')
            var fieldType = splits(3)
            val sql = s"ALTER TABLE $namespace\nADD COLUMNS (\n    $fieldName2 $fieldType FIRST\n  )"
            println(sql)
          }
        }else {
          if (!fieldStr1.contains(" " + fields2.get(x).name() + " ")) {
            var fieldName1 = fields2.get(x - 1).name()
            var fieldName2 = fields2.get(x).name()
            var splits = fields2.get(x).schema().toString().split('"')
            var fieldType = splits(3)
            val sql = s"ALTER TABLE $namespace\nADD COLUMNS (\n    $fieldName2 $fieldType AFTER $fieldName1\n  )"
            println(sql)
          }
        }
      }

      //删除字段，新的schema对旧的schema（遍历旧的schema的字段名）做包含判断，不包含则为删除字段
      for (x <- 0 to fields1.size()-1 ) {
        if (!fieldStr2.contains(" " + fields1.get(x).name() + " ")) {
          val fieldName1 = fields1.get(x).name()
          val sql = s"ALTER TABLE $namespace\nDROP COLUMNS (\n    $fieldName1 \n  )"
          println(sql)
        }
      }

    }





  }

}