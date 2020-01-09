package com.peng.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * DataFrame特点
  * DataFrame是为数据提供了Schema的视图.可以把它当做数据库中的一张表来对待
  * DataFrame也是懒执行的
  * 定制化内存管理,数据以二进制的方式存在于非堆内存,节省了大量空间之外,还摆脱了GC的限制
  * 优化的执行计划,查询计划通过Spark catalyst optimiser进行优化,比如filter下推、裁剪
  * Dataframe的劣势在于在编译期缺少类型安全检查,导致运行时出错
  *
  * DataSet特点
  * 是Dataframe API的一个扩展,是Spark最新的数据抽象
  * 用户友好的API风格,既具有类型安全检查也具有Dataframe的查询优化特性
  * Dataset支持编解码器,当需要访问非堆上的数据时可以避免反序列化整个对象,提高了效率
  * 样例类被用来在Dataset中定义数据的结构信息,样例类中每个属性的名称直接映射到DataSet中的字段名称
  * Dataframe是Dataset的特列,DataFrame=Dataset[Row] ,所以可以通过as方法将Dataframe转换为Dataset.Row是一个类型,跟Car,Person这些的类型一样,所有的表结构信息我都用Row来表示
  * DataSet是强类型的.比如可以有Dataset[Car],Dataset[Person]
  * DataFrame只是知道字段,但是不知道字段的类型,所以在执行这些操作的时候是没办法在编译的时候检查是否类型失败的,比如你可以对一个String进行减法操作,在执行的时候才报错,
  * 而DataSet不仅仅知道字段,而且知道字段类型,所以有更严格的错误检查.就跟JSON对象和类对象之间的类比
  */
object RDDtoDSorDF extends App {

    //创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("RDDtoDSorDF").setMaster("local[2]")

    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //构建SparkContext对象
    val sparkContext = spark.sparkContext

    //构建数据源生成DataFrame
    val dataRDD: RDD[String] = sparkContext.textFile("spark-sql/testdata/rawdata/saveResult/data.text")

    //隐式转换
    import spark.implicits._

    //RDD转换成DF
    val dataDF: DataFrame = dataRDD.toDF()
    dataDF.foreach(println(_))

    //RDD转换成DS
    val dataDS = dataRDD.toDS()
    dataDS.foreach(println(_))
    spark.stop()
}
