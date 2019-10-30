package com.peng.implicit_demo

class Man(val name: String)

class SuperMan(val name: String) {
	def heat(): Unit = print("超人打怪兽")
}

object SuperMan {
	//隐式转换
	implicit def man2SuperMan(man: Man): SuperMan = new SuperMan(man.name)

	def main(args: Array[String]): Unit = {
		//实例化的时候,会把man注入man2SuperMan方法,得到增强后的实例
		val hero = new Man("hero")
		//man具备了SuperMan的方法
		hero.heat
	}
}
