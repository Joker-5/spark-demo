package com.john.doe.wc

import com.john.doe.constant.FilePathConstant
import com.john.doe.utils.SparkUtils

/**
 * Created by joker on 2022/12/9.
 */
object WordNeighborCount {
  // 统计相邻单词共现的次数
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc

    val rdd = sc.textFile(FilePathConstant.wordCountFilePath)

    rdd.flatMap(x => {
      // 将整列按空格拆分
      val line = x.split(" ")
      // 相邻单词拼接
      for (i <- 0 until line.length - 1) yield line(i) + "-" + line(i + 1)
    })
      .filter(x => !x.equals(""))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .map { case (x, y) => (y, x) }
      .sortByKey(ascending = false)
      .take(5)
      .foreach(println)
  }
}
