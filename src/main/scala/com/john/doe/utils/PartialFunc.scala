package com.john.doe.utils

/**
 * Created by joker on 2022/12/9.
 */
object PartialFunc {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, "asd")

    list.collect {
      case x: Int => x + 1
    }
  }
}
