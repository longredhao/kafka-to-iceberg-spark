package org.apache.iceberg.streaming

import org.scalatest.FunSuite
import  com.fasterxml.jackson.databind.Module

class Kafka2IcebergTest extends FunSuite {

    test("testMain") {
      val argStr = "runEnv=test master=local[3] confKey=mysql:B1%G1%tbl_test1^tbl_test2 batchDuration=15  "
       Kafka2Iceberg.main(argStr.split(" "))
    }

}
