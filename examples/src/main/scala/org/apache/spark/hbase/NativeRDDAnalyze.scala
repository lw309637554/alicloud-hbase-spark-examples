package org.apache.spark.hbase

/**
 * Created by admin on 1/19/18.
 */

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object NativeRDDAnalyze {


  def runAction(actionType: String, hBaseRDD: RDD[_], action: RDD[_] => Unit): Unit = {

  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("NativeRDDAnalyze")
//      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "shcExampleBigTable")

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    println("table count is " + hBaseRDD.count())

    //1.filter scan
    val startTime = System.currentTimeMillis()
    val countRDD = hBaseRDD.map(item => {
      item._2
    }).filter(record => (Bytes.toString(record.getRow) <= "row005"))
      .filter(record => Bytes.toBoolean(record.getValue("cf1".getBytes, "col1".getBytes)))
      .map(
        record => {
          (Bytes.toString(record.getRow) + " " + (Bytes.toBoolean(
            record.getValue("cf1".getBytes, "col1".getBytes))).toString)
        }
      )
    println("filter scan count:" + countRDD.count())
    val endTime = System.currentTimeMillis()
    println("action cost time:" + ((endTime - startTime)/1000.0) + "s")
    spark.stop()
  }
}
