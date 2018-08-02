package org.apache.spark.structstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
  * Created by admin on 1/19/18.
  */

object StructStreamingWordCount {


  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("StructStreamingWordCount")
      .config("spark.sql.streaming.unsupportedOperationCheck", false)
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val catalog =
      s"""{
         |"table":{"namespace":"default", "name":"structStreamingCount2", "tableCoder":"PrimitiveType"},
         |"rowkey":"key",
         |"columns":{
         |"value":{"cf":"rowkey", "col":"key", "type":"string"},
         |"count":{"cf":"cf1", "col":"count", "type":"long"}
         |}
         |}""".stripMargin

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count().filter($"value"=!="")


    val query = wordCounts.
      writeStream.
      queryName("hbase writer").
      format("org.apache.spark.sql.execution.datasources.hbase.HBaseSinkProvider").
      option("checkpointLocation", "file:///Users/liwei/work-space/spark/test6/").
      option("hbasecat", catalog).
      outputMode(OutputMode.Update()).
      trigger(Trigger.ProcessingTime("10 seconds")).
      start


    //    val query = wordCounts.writeStream
    //      .outputMode("complete")
    //      .format("console")
    //      .start()


    query.awaitTermination()
    spark.stop()
  }
}
