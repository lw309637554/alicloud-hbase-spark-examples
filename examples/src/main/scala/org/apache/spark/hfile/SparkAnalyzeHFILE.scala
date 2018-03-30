package org.apache.spark.hfile


import java.util.Calendar

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

object SparkReadHBaseSnapshot{

  case class hVar(rowkey: Int, colFamily: String, colQualifier: String, colDatetime: Long, colDatetimeStr: String, colType: String, colValue: String)

  def main(args: Array[String]) {
    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)

    val props = getProps(args(0))
    val max_versions : Int = props.getOrElse("hbase.snapshot.versions","3").toInt

    val sparkConf = new SparkConf().setAppName("SparkReadHBaseSnapshot")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    println("[ *** ] Creating HBase Configuration")
    val hConf = HBaseConfiguration.create()


    hConf.set("hbase.rootdir", props.getOrElse("hbase.rootdir", "/hbase"))
    //配置用户的云HBase实例的地址
    hConf.set("hbase.zookeeper.quorum",  props.getOrElse("hbase.zookeeper.quorum", ""))
    hConf.set(TableInputFormat.SCAN, convertScanToString(new Scan().setMaxVersions(max_versions)) )
    //首先用户需要开通云HBase的HDFS访问权限，然后参考用户文档配置hdfs访问的配置hdfs-site.xml
    hConf.addResource(this.getClass().getClassLoader().getResource("hdfs-site.xml"))
    val job = Job.getInstance(hConf)

    val path = new Path(props.getOrElse("hbase.snapshot.path", "/snapshot"))
    //配置对应的snapshot表的名字
    val snapName = props.getOrElse("hbase.snapshot.name", "test_snapshot")

    TableSnapshotInputFormat.setInput(job, snapName, path)

    val hBaseRDD = sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableSnapshotInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val result = hBaseRDD
      .map(item => {
        item._2
      }).map(record=> record.getValue("cf2".getBytes, "col2".getBytes)).take(2)
      result.foreach(println)

    val record_count_raw = hBaseRDD.count()
    println("[ *** ] Read in SnapShot (" + snapName.toString  + "), which contains " + record_count_raw + " records")
    sc.stop()

  }


  def convertScanToString(scan : Scan) = {
    val proto = ProtobufUtil.toScan(scan);
    Base64.encodeBytes(proto.toByteArray());
  }


  def getArrayProp(props: => HashMap[String,String], prop: => String): Array[String] = {
    return props.getOrElse(prop, "").split(",").filter(x => !x.equals(""))
  }


  def getProps(file: => String): HashMap[String,String] = {
    var props = new HashMap[String,String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }

}