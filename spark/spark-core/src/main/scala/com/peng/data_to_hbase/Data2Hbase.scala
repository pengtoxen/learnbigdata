package com.peng.data_to_hbase

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过spark读取数据文件，把结果数据保存到hbase表中
  */
object Data2Hbase {

    def main(args: Array[String]): Unit = {
        //1、创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("Data2Hbase").setMaster("local[2]")

        //2、构建SparkContext
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("warn")

        //3、读取文件数据
        val usersRDD: RDD[Array[String]] = sc.textFile("spark-core/testdata/rawdata/data_to_hbase/users.dat").map(x => x.split("::"))

        //4、保存结果数据到hbase表中
        usersRDD.foreachPartition(iter => {

            //4.1 获取hbase的数据库连接
            val configuration: Configuration = HBaseConfiguration.create()
            //指定zk集群的地址
            configuration.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181")
            val connection: Connection = ConnectionFactory.createConnection(configuration)

            //4.2 对于hbase表进行操作这里需要一个Table对象
            val table: Table = connection.getTable(TableName.valueOf("person"))

            //4.3 把数据保存在表中
            try {
                iter.foreach(x => {
                    val put = new Put(x(0).getBytes)
                    val puts = new util.ArrayList[Put]()
                    //构建数据
                    val put1: Put = put.addColumn("f1".getBytes, "gender".getBytes, x(1).getBytes)
                    val put2: Put = put.addColumn("f1".getBytes, "age".getBytes, x(2).getBytes)
                    val put3: Put = put.addColumn("f2".getBytes, "position".getBytes, x(3).getBytes)
                    val put4: Put = put.addColumn("f2".getBytes, "code".getBytes, x(4).getBytes)

                    puts.add(put1)
                    puts.add(put2)
                    puts.add(put3)
                    puts.add(put4)

                    //提交数据
                    table.put(puts)
                })
            } catch {
                case e: Exception => e.printStackTrace()
            } finally {
                if (connection != null) {
                    connection.close()
                }
            }
        })

        sc.stop()
    }
}
