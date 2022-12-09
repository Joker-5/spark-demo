package com.john.doe.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Created by joker on 2022/12/9.
 */
object SparkUtils {
  private val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("createRDD")
    .getOrCreate()

  def getEnv: SparkSession = spark

  def getSc: SparkContext = spark.sparkContext

}
