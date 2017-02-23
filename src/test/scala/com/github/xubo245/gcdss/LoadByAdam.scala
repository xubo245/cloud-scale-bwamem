package com.github.xubo245.gcdss

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.rdd.ADAMContext

/**
  * Created by xubo on 2017/2/22.
  * run success
  */
object LoadByAdam {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org.apache.parquet.hadoop").setLevel(Level.ERROR)
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    println("start:")
    //    val output = "hdfs://219.219.220.149:9000/xubo/project/alignment/cs-bwamem/input/fastq/newg38L50c10000Nhs20Paired12P1k2.adam"  //success
    val output = "hdfs://219.219.220.149:9000/xubo/project/alignment/cs-bwamem/input/fastq/newg38L50c1000000Nhs20Paired12P16bn5000000t1k1.adam/" //success
    //    val output = "hdfs://219.219.220.149:9000/xubo/project/alignment/cs-bwamem/input/fastq/newg38L50c10000000Nhs20Paired12P64bn200000000k106.adam"  //success
    //    val output = "hdfs://219.219.220.149:9000/xubo/project/alignment/cs-bwamem/input/fastq/newg38L50c1000000Nhs20Paired12P16bn5000000k120.adam" //success
    //        val output = "hdfs://219.219.220.149:9000/xubo/project/alignment/cs-bwamem/input/fastq/newg38L50c10000Nhs20Paired12P8.fastq"  //fail


    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val ac = new ADAMContext(sc)
    import org.bdgenomics.adam.rdd.ADAMContext._
    val df3 = ac.loadAlignments(output)
    //    val df3 = sqlContext.read.option("mergeSchema", "true").parquet(output + "/0")
    //    val df3 = sqlContext.read.option("mergeSchema", "true").parquet(output + "/0")
    //    df3.printSchema()
    //    df3.show()
    df3.take(10).foreach(println)
    df3.take(10).foreach { each =>
      println(each.start)
    }
    println(df3.count())
    println("end")
  }
}
