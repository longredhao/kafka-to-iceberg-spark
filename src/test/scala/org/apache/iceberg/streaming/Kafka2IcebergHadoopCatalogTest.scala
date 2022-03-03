package org.apache.iceberg.streaming

import org.scalatest.FunSuite
import  com.fasterxml.jackson.databind.Module

class Kafka2IcebergHadoopCatalogTest extends FunSuite {

    test("testMain") {
      val argStr = "runEnv=test master=local[3] confKey=mysql:B0%G1 batchDuration=15  "
       Kafka2Iceberg.main(argStr.split(" "))
    }

}
