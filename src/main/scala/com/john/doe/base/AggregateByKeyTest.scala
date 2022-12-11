package com.john.doe.base

import com.john.doe.constant.FilePathConstant
import com.john.doe.utils.SparkUtils

/**
 * Created by joker on 2022/12/11.
 */
object AggregateByKeyTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc

    val rdd = sc.textFile(FilePathConstant.wordCountFilePath)

    rdd.flatMap(x => x.split(" "))
      .filter(x => !x.equals(""))
      .map(x => (x, 1))
      // 先求和，在求最大值
      .aggregateByKey(0)((x, y) => x + y, (x, y) => math.max(x, y))
      .map { case (x, y) => (y, x) }
      .sortByKey(ascending = false)
      .take(5)
      .foreach(println)
  }
}
