package com.john.doe.base

import com.john.doe.constant.FilePathConstant
import com.john.doe.utils.SparkUtils

import scala.util.Random.nextInt

/**
 * Created by joker on 2022/12/11.
 */
object ReduceByKeyTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc

    val rdd = sc.textFile(FilePathConstant.wordCountFilePath)

    rdd.flatMap(x => x.split(" "))
      .filter(x => !x.equals(""))
      .map(x => (x, nextInt(100)))
      // 统计 nextInt 随机最大映射的元素
      .reduceByKey((x, y) => math.max(x, y))
      .map { case (x, y) => (y, x) }
      .sortByKey(ascending = false)
      .take(5)
      .foreach(println)
  }
}
