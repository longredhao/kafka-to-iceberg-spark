# Spark Shell Iceberg 使用测试

## Spark shell 启动

要在 Spark shell 中使用 Iceberg，请使用以下--packages选项：
其中  omit uri to use the same URI as Spark: hive.metastore.uris in hive-site.xml  
```shell

/opt/run/spark3/bin/spark-shell --packages org.apache.iceberg:iceberg-spark3-runtime:0.13.0  \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.hive_prod.type=hive \
--conf spark.sql.catalog.hive_prod.uri=thrift://hadoop:9083

/opt/run/spark3/bin/spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.1  \
spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog \
spark.sql.catalog.hive_prod.type = hive \
spark.sql.catalog.hive_prod.uri = thrift://hadoop:9083

```


    val df1 = Seq(
      (1, "Karol", 19),
      (2, "Abby", 20),
      (3, "Zena", 18)
    ).toDF("id", "name", "age")