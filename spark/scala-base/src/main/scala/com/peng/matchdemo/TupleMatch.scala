package com.peng.matchdemo

object TupleMatch extends App {
	//定义一个数组
	val tuple = (1,2,3)
	//匹配元祖
	tuple match {
		case (1,x,y) => println(s"1,$x,$y")
		case (2,x,y) => println(s"$x,$y")
		case _ => println("something else")
	}
}
