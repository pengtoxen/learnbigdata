package com.peng.partitioner_sort

import org.apache.spark.Partitioner

class MyPartitioner(i: Int) extends Partitioner {
	//指定总的分区数
	override def numPartitions: Int = {
		i
	}

	//数据按照指定的某种规则进入到指定的分区号中
	override def getPartition(key: Any): Int = {
		//这里的key就是单词
		val length: Int = key.toString.length
		length match {
			case 4 => 0
			case 5 => 1
			case 6 => 2
			case _ => 0
		}
	}
}
