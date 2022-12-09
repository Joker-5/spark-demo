package com.john.doe.grammar

/**
 * Created by joker on 2022/12/9.
 */
object YieldTest {
  def main(args: Array[String]): Unit = {
    val list = List(1, 3, 4, 5, 6)
    // 利用 yield 关键字生成新集合
    val doubleList = for (x <- list) yield x * 2

    doubleList.foreach(x => print(s"${x} "))
  }
}
