package com.peng.match_demo

import scala.util.Random

object TypeMatch extends App {
	//定义一个数组
	val arr = Array("hadoop", 1, 2.0, TypeMatch)
	//随机取数组中的一位,使用Random.nextInt
	val value = arr(Random.nextInt(arr.length))
	//匹配类型
	value match {
		case v: Int => println("Int=>" + v)
		case v: Double if v >= 0 => println("Double=>" + v)
		case v: String => println("String=>" + v)
		case _ => throw new Exception("not match type")
	}
}
