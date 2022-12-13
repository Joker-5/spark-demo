package com.john.doe.base

import com.john.doe.constant.FilePathConstant
import com.john.doe.utils.SparkUtils

/**
 * Created by joker on 2022/12/13.
 */
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc

    val rdd = sc.textFile(FilePathConstant.wordCountFilePath)
    val accumulator = sc.longAccumulator("Empty String")

    def f(x: String): Boolean = {
      if (x.equals("")) {
        accumulator.add(1)
        return false
      } else {
        return true
      }
    }


    rdd.flatMap(x => x.split(" "))
      .filter(f)
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .map { case (x, y) => (y, x) }
      .sortByKey(ascending = false)
      .take(5)
      .foreach(println)

    println(s"empty string count: ${accumulator.value}")
  }
  
}
