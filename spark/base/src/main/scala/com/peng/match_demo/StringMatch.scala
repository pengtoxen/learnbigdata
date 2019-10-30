package com.peng.match_demo

import scala.util.Random

object StringMatch extends App {
	//定义一个数组
	val arr = Array("hadoop", "zookeeper", "spark", "hive")
	//随机取数组中的一位,使用Random.nextInt
	val name = arr(Random.nextInt(arr.length))
	//匹配字符串
	name match {
		case "hadoop" => println("hadoop")
		case "zookeeper" => println("zookeeper")
		case "spark" => println("spark")
		case _ => println("unknown")
	}
}
