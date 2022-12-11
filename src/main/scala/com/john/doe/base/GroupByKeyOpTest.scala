package com.john.doe.base

import com.john.doe.constant.FilePathConstant
import com.john.doe.utils.SparkUtils

/**
 * Created by joker on 2022/12/11.
 */
object GroupByKeyOpTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc

    val rdd = sc.textFile(FilePathConstant.wordCountFilePath)

    rdd.flatMap(x => x.split(" "))
      .filter(x => !x.equals(""))
      .map(x => (x, x))
      .reduceByKey((x, y) => x + y)
      .groupByKey() // 按照单词做分组收集，将相同的单词收集在一起
      .take(5)
      .foreach(println)
  }
}
