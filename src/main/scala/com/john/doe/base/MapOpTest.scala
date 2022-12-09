package com.john.doe.base

import com.john.doe.constant.FilePathConstant
import com.john.doe.utils.SparkUtils
import org.slf4j.{Logger, LoggerFactory}

import java.security.MessageDigest

/**
 * Created by joker on 2022/12/9.
 */
object MapOpTest {

  private val log: Logger = LoggerFactory.getLogger(MapOpTest.getClass)

  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc

    val rdd = sc.textFile(FilePathConstant.wordCountFilePath)

    rdd.flatMap(x => x.split(" "))
      .filter(x => !x.equals(""))
      .map { x =>
        val md5 = MessageDigest.getInstance("MD5")
        val hash = md5.digest(x.getBytes).mkString
        (hash, 1)
      }.reduceByKey((x, y) => x + y)
      .map { case (k, v) => (v, k) }
      .sortByKey(ascending = false)
      .take(5)
      .foreach(x => log.error(x.toString()))
  }
}
