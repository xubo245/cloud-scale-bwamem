package com.github.xubo245.gcdss

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xubo on 2017/2/22.
  */
//import org.scalatest.FunSuite
object TestFormats {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    println("start:")
//    val output = "../file\\alignment\\CSBWAMEMformats\\newg38L50c10000Nhs20Paired12P1k1.adam" //success
//    val output = "hdfs://219.219.220.149:9000/xubo/project/alignment/cs-bwamem/input/fastq/newg38L50c10000Nhs20Paired12P1k2.adam"  //success
    val output = "hdfs://219.219.220.149:9000/xubo/project/alignment/cs-bwamem/input/fastq/newg38L50c10000Nhs20Paired12P8.fastq"  //success
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet(output + "/0")
    //    val df3 = sqlContext.read.option("mergeSchema", "true").parquet(output + "/0")
    //    df3.printSchema()
    df3.show()
    df3.take(10).foreach(println)
    println(df3.count())
    println("end")
  }
}
