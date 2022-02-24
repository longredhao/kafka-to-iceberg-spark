

## Schema  管理说明

Schema 信息 维护 (使用 Avro Schema 作为 Schema 信息的综合转化节点)
1、 Schema 保存:  Avro Schema -> Avro Schema String -> Persistent to MySQL 
2、 Schema 恢复:  MySQL -> Load Schema String  -> Parse String to Avro Schema
3、 Iceberg Schema 产生： Avro Schema -> Iceberg Schema -> Write Data to Iceberg
4、 Schema 更新： Old Avro Schema -> New Avro Schema -> Iceberg Schema
5、 Schema 更新检测: 
 - Avro GenericRecord Schema -> Schema HashCode 
 - SchemaAccumulator -> Schema Version -> Schema -> Schema HashCode








## Spark Structure Streaming 

## Example Data Format  
```text
{"before": null, "after": {"ID": 1, "C1": "v1", "C2": "v2", "C3": 1, "C4": 12, "CREATE_TIME": 1645157193000, "UPDATE_TIME": 1645157193000}, "source": {"version": "1.8.0.Final", "connector": "mysql", "name": "test", "ts_ms": 1645157221283, "snapshot": "last", "db": "db_gb18030_test", "sequence": null, "table": "tbl_test", "server_id": 0, "gtid": null, "file": "mysql-bin.000003", "pos": 57965, "row": 0, "thread": null, "query": null}, "op": "r", "ts_ms": 1645157221285, "transaction": null} 

{"before":{"test.db_gb18030_test.tbl_test.Value":{"ID":1,"C1":{"string":"v1"},"C2":"v2","C3":{"int":4},"C4":{"long":12},"CREATE_TIME":1645157193000,"UPDATE_TIME":1645163880000}},"after":{"test.db_gb18030_test.tbl_test.Value":{"ID":1,"C1":{"string":"v1"},"C2":"v2","C3":{"int":3},"C4":{"long":12},"CREATE_TIME":1645157193000,"UPDATE_TIME":1645163889000}},"source":{"version":"1.8.0.Final","connector":"mysql","name":"test","ts_ms":1645163889000,"snapshot":{"string":"false"},"db":"db_gb18030_test","sequence":null,"table":{"string":"tbl_test"},"server_id":1,"gtid":{"string":"e45b718e-906f-11ec-89e3-0242c0a8640a:117"},"file":"mysql-bin.000003","pos":60900,"row":0,"thread":null,"query":null},"op":"u","ts_ms":{"long":1645164439953},"transaction":{"io.confluent.connect.avro.ConnectDefault":{"id":"e45b718e-906f-11ec-89e3-0242c0a8640a:117","total_order":1,"data_collection_order":1}}}
{"before":{"test.db_gb18030_test.tbl_test.Value":{"ID":1,"C1":{"string":"v1"},"C2":"v2","C3":{"int":3},"C4":{"long":12},"CREATE_TIME":1645157193000,"UPDATE_TIME":1645163889000}},"after":{"test.db_gb18030_test.tbl_test.Value":{"ID":1,"C1":{"string":"v1"},"C2":"v2","C3":{"int":4},"C4":{"long":12},"CREATE_TIME":1645157193000,"UPDATE_TIME":1645163889000}},"source":{"version":"1.8.0.Final","connector":"mysql","name":"test","ts_ms":1645163889000,"snapshot":{"string":"false"},"db":"db_gb18030_test","sequence":null,"table":{"string":"tbl_test"},"server_id":1,"gtid":{"string":"e45b718e-906f-11ec-89e3-0242c0a8640a:117"},"file":"mysql-bin.000003","pos":61082,"row":0,"thread":null,"query":null},"op":"u","ts_ms":{"long":1645164439954},"transaction":{"io.confluent.connect.avro.ConnectDefault":{"id":"e45b718e-906f-11ec-89e3-0242c0a8640a:117","total_order":2,"data_collection_order":2}}}

+-------------------+--------------------+--------------------+---------+------+--------------------+-------------+
|                key|               value|               topic|partition|offset|           timestamp|timestampType|
+-------------------+--------------------+--------------------+---------+------+--------------------+-------------+
|[00 00 00 00 03 02]|[00 00 00 00 04 0...|test.db_gb18030_t...|        0|     0|2022-02-18 12:07:...|            0|
|[00 00 00 00 03 02]|[00 00 00 00 04 0...|test.db_gb18030_t...|        0|     1|2022-02-18 12:07:...|            0|
|[00 00 00 00 03 02]|[00 00 00 00 04 0...|test.db_gb18030_t...|        0|     2|2022-02-18 13:57:...|            0|
|[00 00 00 00 03 02]|[00 00 00 00 04 0...|test.db_gb18030_t...|        0|    11|2022-02-18 14:07:...|            0|
+-------------------+--------------------+--------------------+---------+------+--------------------+-------------+

```





## Spark Iceberg 常用命令
### Spark Shell 
要在 Spark shell 中使用 Iceberg，请使用以下--packages选项： 
```shell
/opt/run/spark3/bin/spark-shell --packages org.apache.iceberg:iceberg-spark3-runtime:0.13.0  
```

## Spark SQL 
```shell
/opt/run/spark3/bin/spark-sql --packages org.apache.iceberg:iceberg-spark3-runtime:0.13.0 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
--conf spark.sql.catalog.spark_catalog.type=hive \
--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.local.type=hadoop \
--conf spark.sql.catalog.local.warehouse=$HOME/temp/spark-iceberg/warehouse

```
spark-sql --packages org.apache.iceberg:iceberg-spark3-runtime:0.13.0 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
--conf spark.sql.catalog.spark_catalog.type=hive \
--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.local.type=hadoop \
--conf spark.sql.catalog.local.warehouse=$HOME/Works/Temp/spark-iceberg/warehouse