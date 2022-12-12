package com.john.doe.base

import com.john.doe.constant.FilePathConstant
import com.john.doe.utils.SparkUtils

import java.security.MessageDigest

/**
 * Created by joker on 2022/12/12.
 */
object RDDCacheTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc

    val rdd = sc.textFile(FilePathConstant.wordCountFilePath)

    val mapRDD = rdd.flatMap(x => x.split(" "))
      .filter(x => !x.equals(""))
      .map { x => (x, 1) }
      .reduceByKey((x, y) => x + y)
      .map { case (k, v) => (v, k) }
    // Cache RDD
    mapRDD.cache
    // 触发缓存
    mapRDD.count

    mapRDD.sortByKey(ascending = true)
      .take(5)

    val targetPath = "src/main/resources/target"
    mapRDD.saveAsTextFile(targetPath)
  }
}
