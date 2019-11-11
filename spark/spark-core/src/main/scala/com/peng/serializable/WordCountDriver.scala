package com.peng.serializable

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * address需要继承Serializable接口才能序列化后在网络中传输
  */
/**
  * (1) 如果函数中使用了该类对象，该类要实现序列化类,继承接口Serializable
  * (2) 如果函数中使用了该类对象的成员变量，该类除了要实现序列化之外，所有的成员变量必须要实现序列化
  * (3) 对于不能序列化的成员变量使用“@transient”标注，告诉编译器不需要序列化
  * (4) 也可将依赖的变量独立放到一个小的class中，让这个class支持序列化，这样做可以减少网络传输量，提高效率
  * (5) 可以把对象的创建直接在该函数中构建这样避免需要序列化
  */
class Address extends Serializable {
	val name = "beijing"
	//connection对象不支持序列化
	//address对象序列化的时候,发现这个connection不支持序列化就会报错
	//这时候可以加注解@transient,告诉编译器不需要序列化
	@transient
	val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "root")
}

object WordCountDriver {
	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
		val sc = new SparkContext(sparkConf)
		//设置日志输出级别
		sc.setLogLevel("error")
		//读取数据文件
		val data: RDD[Int] = sc.parallelize(1 to 10)
		/**
		  * （1）代码中address对象在driver本地序列化
		  * （2）对象序列化后传输到远程executor节点
		  * （3）远程executor节点反序列化对象
		  * （4）最终远程节点执行
		  */
		val address = new Address
		val result = data.map(x => {
			(x, address.name)
		})

		/**
		  * 输出结果
		  * (1,beijing)
		  * (6,beijing)
		  * (2,beijing)
		  * (7,beijing)
		  * (3,beijing)
		  * (8,beijing)
		  * (4,beijing)
		  * (9,beijing)
		  * (5,beijing)
		  * (10,beijing)
		  */
		result.foreach(println)
		sc.stop()
	}
}
