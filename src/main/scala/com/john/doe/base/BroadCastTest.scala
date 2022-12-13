package com.john.doe.base

import com.john.doe.utils.SparkUtils

/**
 * Created by joker on 2022/12/12.
 */
object BroadCastTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc

    val list = List("a", "b")
    // 创建广播变量
    val bc = sc.broadcast(list)
    // 下面两种方式读取的结果是一致的：List(a, b)
    println(s"读取广播变量：${bc.value}， 直接读取原变量：${list}")
  }
}
