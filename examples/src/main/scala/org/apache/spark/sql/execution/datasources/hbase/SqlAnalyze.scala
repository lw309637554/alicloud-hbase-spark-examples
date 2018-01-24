package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * Created by admin on 1/19/18.
 */
object SqlAnalyze {
  val cat =
    s"""{
       |"table":{"namespace":"default", "name":"shcExampleBigTable", "tableCoder":"PrimitiveType"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
       |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
       |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
       |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
       |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
       |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
       |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
       |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
       |}
       |}""".stripMargin

  def runAction(actionType: String, hBaseDataFrame: DataFrame, action: DataFrame => Unit): Unit = {

    action(hBaseDataFrame)

  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("SqlAnalyze")
//      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }
    val df = withCatalog(cat)
    println("table count is " + df.count())

    //1.filter scan
    val startTime = System.currentTimeMillis()
    val countDf = df.filter($"col0" <= "row005").filter($"col1")
      .select($"col0", $"col1")
    val countResult = countDf.count
    println("filter scan count:" + countResult)
    val endTime = System.currentTimeMillis()
    println("action cost time:" + ((endTime - startTime)/1000.0) + "s")

    spark.stop()
  }
}
