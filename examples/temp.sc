import org.apache.avro.Schema.Parser

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
val schema1 = new Parser().parse(schemaStr1)


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
val schema2 = new Parser().parse(schemaStr2)


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
val schema3 = new Parser().parse(schemaStr3)


def getAlterTableSql(schema1: String, schema2:String): String = {
 //todo
  null
}

