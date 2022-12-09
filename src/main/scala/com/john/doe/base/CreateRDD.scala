package com.john.doe.base

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Created by joker on 2022/12/9.
 */
object CreateRDD {
  private val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("createRDD")
    .getOrCreate()

  private val sc: SparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {
    val list = List("a", "s", "d")

    val rdd = sc.parallelize(list)
  }
}
