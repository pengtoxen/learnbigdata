package com.peng.matchdemo

object ArrayMatch extends App {
	//定义一个数组
	val arr = Array(1,2,4,5)
	//匹配数组
	arr match {
		case Array(1, x, y) => println(x + "---" + y)
		case Array(1, _*) => println("1....")
		case Array(0) => println("only 0")
		case _ => println("something else")
	}
}
