package com.peng.data_to_mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Data2MysqlForeachPartitions {
    def main(args: Array[String]): Unit = {
        //1、构建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("Data2MysqlForeachPartitions").setMaster("local[2]")

        //2、构建SparkContext
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("warn")

        //3、读取数据文件
        val data: RDD[String] = sc.textFile("E:\\data\\person.txt")

        //4、切分每一行    // id  name  age
        val personRDD: RDD[(String, String, Int)] = data.map(x => x.split(",")).map(x => (x(0), x(1), x(2).toInt))

        //5、把数据保存到mysql表中
        //使用foreachPartition每个分区建立一次链接，减少与mysql链接次数
        personRDD.foreachPartition(iter => {
            //把数据插入到mysql表操作
            //1、获取连接
            val connection: Connection = DriverManager.getConnection("jdbc:mysql://node1:3306/spark", "root", "root")

            //2、定义插入数据的sql语句
            val sql = "insert into people(id,name,age) values(?,?,?)"

            //3、获取PreParedStatement

            try {
                val ps: PreparedStatement = connection.prepareStatement(sql)

                //4、获取数据,给？号 赋值
                iter.foreach(line => {

                    ps.setString(1, line._1)
                    ps.setString(2, line._2)
                    ps.setInt(3, line._3)
                    ps.execute()
                })

            } catch {
                case e: Exception => e.printStackTrace()
            } finally {
                if (connection != null) {
                    connection.close()
                }

            }
        })


    }
}
