package com.john.doe.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by joker on 2022/12/8.
 */
object WordCount {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local")
      .appName("wordCount")
      .getOrCreate()

    // 读取文件内容
    val lineRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/wikiOfSpark.txt")

    // 以行为单位做分词
    val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))
    val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))

    // 把RDD元素转换为（Key，Value）的形式
    val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))
    // 按照单词做分组计数
    val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)
    // 打印词频最高的10个词汇
    wordCounts.map(
      { case (k, v) => (v, k) }
    ).sortByKey(ascending = false)
      .take(10)
      .foreach(println(_))
  }
}
