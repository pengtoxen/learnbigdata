package com.peng.match_demo

object ListMatch extends App {
    //定义一个数组
    val list = List(0, 2, 3)
    //匹配集合
    list match {
        case 0 :: Nil => println("only 0")
        case 0 :: tail => println("0....")
        case x :: y :: z :: Nil => println(s"x:$x y:$y z:$z")
        case _ => println("something else")
    }
}
