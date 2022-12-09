package com.john.doe.base

import com.john.doe.base.MapOpTest.log
import com.john.doe.constant.FilePathConstant
import com.john.doe.utils.SparkUtils
import org.slf4j.{Logger, LoggerFactory}

import java.security.MessageDigest

/**
 * Created by joker on 2022/12/9.
 */
object MapPartitionsOpTest {
  private val log: Logger = LoggerFactory.getLogger(MapOpTest.getClass)

  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc

    val rdd = sc.textFile(FilePathConstant.wordCountFilePath)

    rdd.flatMap(x => x.split(" "))
      .filter(x => !x.equals(""))
      .mapPartitions(partition => { // 以分区为单位计算
        val md5 = MessageDigest.getInstance("MD5") // 每个分区仅实例化一次
        val newPartition = partition.map(x => {
          (md5.digest(x.getBytes).mkString, 1)
        })
        newPartition
      })
      .reduceByKey((x, y) => x + y)
      .map { case (k, v) => (v, k) }
      .sortByKey(ascending = false)
      .take(5)
      .foreach(x => log.error(x.toString()))
  }
}
