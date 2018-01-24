package org.apache.spark.phoenix.connector
import org.apache.spark.sql.{SparkSession, SQLContext}

/**
 * Created by liwei.li on 11/25/17.
 */
object PhoenixTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("PhoenixTest")
//      .master("local")
      .getOrCreate()
    val sqlContext = spark.sqlContext
    val df = sqlContext.load(
      "org.apache.phoenix.spark",
      Map("table" -> "TABLE1", "zkUrl" ->
        "")
    )

    df
      .filter(df("COL1") === "test_row_1" && df("ID") === 1L)
      .select(df("ID"))
      .show
  }
}
