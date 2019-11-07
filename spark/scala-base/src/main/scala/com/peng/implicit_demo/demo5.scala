package com.peng.implicit_demo

import java.io.File
import scala.io.Source

object MyPredef {
	implicit def file2RichFile(file: File): RichFile = new RichFile(file)
}

class RichFile(file: File) {
	def read(): String = {
		val fis = Source.fromFile(file)
		fis.mkString
	}
}

object RichFile {
	def main(args: Array[String]): Unit = {
		//1.构建一个File对象
		val file = new File("F:\\learnbigdata\\spark\\base\\testdata\\rawdata\\test.txt")
		//2.手动导入隐式转换
		import MyPredef.file2RichFile
		//3.调用read方法
		val data: String = file.read()
		//4.打印结果
		print(data)
	}
}
