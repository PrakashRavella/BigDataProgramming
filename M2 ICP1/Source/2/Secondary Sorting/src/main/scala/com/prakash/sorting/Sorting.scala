package com.prakash.sorting

package com.sort

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._

object Sorting {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");
    val conf = new SparkConf().setAppName("Spark - Secondary Sort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile("input.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => ((k(0), k(1)),k(2)) }
    println("pairsRDD")
    pairsRDD.foreach {
      println
    }
    val numReducers = 2;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(d => d))
    println("listRDD")
    listRDD.foreach {
      println
    }
    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
    println("resultRDD")
    resultRDD.foreach {
      println
    }

    resultRDD.saveAsTextFile("data/output")
    sc.stop()
  }
}
