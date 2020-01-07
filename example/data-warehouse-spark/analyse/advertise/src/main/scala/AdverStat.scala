import java.util.Date

import com.peng.commons.conf.ConfigurationManager
import com.peng.commons.constant.Constants
import com.peng.commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdverStat {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        // 设置checkpoint,重启程序的时候,看看checkpoint下有没有序列化后的数据,有的话恢复过来,没有就调用func
        // 创建新的一个streamingContext
        // val streamingContext = StreamingContext.getActiveOrCreate(checkpointDir, func)
        val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

        // kafka集群配置参数
        val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
        val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

        val kafkaParam = Map(
            "bootstrap.servers" -> kafka_brokers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "peng",

            // auto.offset.reset
            // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费
            // earliest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
            // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
            "auto.offset.reset" -> "latest",

            // 开启自动提交，他只会每隔一段时间去提交一次offset
            // 如果你每次要重启一下consumer的话，他一定会把一些数据重新消费一遍
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        // 创建DStream
        // adRealTimeDStream: DStream[RDD RDD RDD ...]  RDD[message]  message: key value
        val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](streamingContext,

            // 多个分区里的数据,均匀的分配给executor
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
        )

        // 对DStream执行map操作其实是对DStream中的RDD执行map操作,item是RDD中每一个元素
        // item(string): timestamp province city userid adid
        val adReadTimeValueDStream = adRealTimeDStream.map(item => item.value())

        val adRealTimeFilterDStream = adReadTimeValueDStream.transform {
            logRDD =>

                // blackListArray: Array[AdBlacklist]     AdBlacklist: userId
                val blackListArray = AdBlacklistDAO.findAll()

                // userIdArray: Array[Long]  [userId1, userId2, ...]
                val userIdArray = blackListArray.map(item => item.userid)

                logRDD.filter {
                    // log : timestamp province city userid adid
                    case log =>
                        val logSplit = log.split(" ")
                        val userId = logSplit(3).toLong
                        !userIdArray.contains(userId)
                }
        }

        // 设置checkpoint目录
        // 10秒checkpoint一次
        streamingContext.checkpoint("./spark-streaming")
        adRealTimeFilterDStream.checkpoint(Duration(10000))

        // 需求一:实时维护黑名单
        generateBlackList(adRealTimeFilterDStream)

        // 需求二:各省各城市一天中的广告点击量(累计统计)
        val key2ProvinceCityCountDStream = provinceCityClickStat(adRealTimeFilterDStream)

        // 需求三:每天每个省份Top3热门广告
        provinceTop3Adver(sparkSession, key2ProvinceCityCountDStream)

        // 需求四:最近1个小时广告统计
        getRecentHourClickCount(adRealTimeFilterDStream)

        streamingContext.start()
        streamingContext.awaitTermination()
    }

    /**
      * 最近1个小时广告统计
      *
      * @param adRealTimeFilterDStream
      */
    def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {
        val key2TimeMinuteDStream = adRealTimeFilterDStream.map {

            // log: timestamp province city userId adid
            case log =>
                val logSplit = log.split(" ")
                val timeStamp = logSplit(0).toLong

                // yyyyMMddHHmm
                val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))
                val adid = logSplit(4).toLong

                val key = timeMinute + "_" + adid

                (key, 1L)
        }

        // 每隔1分钟统计最近60分钟的数据
        val key2WindowDStream = key2TimeMinuteDStream.reduceByKeyAndWindow((a: Long, b: Long) => (a + b), Minutes(60), Minutes(1))

        key2WindowDStream.foreachRDD {
            rdd =>
                rdd.foreachPartition {
                    item =>
                        val trendArray = new ArrayBuffer[AdClickTrend]()
                        for ((key, count) <- item) {
                            val keySplit = key.split("_")
                            val timeMinute = keySplit(0)
                            val date = timeMinute.substring(0, 8)
                            val hour = timeMinute.substring(8, 10)
                            val minute = timeMinute.substring(10)
                            val adid = keySplit(1).toLong

                            trendArray += AdClickTrend(date, hour, minute, adid, count)
                        }

                        AdClickTrendDAO.updateBatch(trendArray.toArray)
                }
        }
    }

    /**
      * 每天每个省份Top3热门广告
      * 这个数据是实时更新的,每有一个RDD产生,数据就会更新一遍
      * 不保留历史数据
      *
      * @param sparkSession
      * @param key2ProvinceCityCountDStream
      */
    def provinceTop3Adver(sparkSession: SparkSession, key2ProvinceCityCountDStream: DStream[(String, Long)]) = {

        // 处理前的数据结构(date_province_city_adid,count)
        // 处理后的数据结构(date_province_adid,count),会有多个key重复,因为一个省包含多个城市
        val key2ProvinceCountDStream = key2ProvinceCityCountDStream.map {
            case (key, count) => {
                val keySplit = key.split("_")
                val date = keySplit(0)
                val province = keySplit(1)
                val adid = keySplit(3)
                val newKey = date + "_" + province + "_" + adid
                (newKey, count)
            }
        }

        val key2ProvinceAggrCountDStream = key2ProvinceCountDStream.reduceByKey(_ + _)

        // 针对每个RDD进行处理
        val top3DStream = key2ProvinceAggrCountDStream.transform {
            rdd => {
                // (date_province_adid,count)
                val basicDateRDD = rdd.map {
                    case (key, count) =>
                        val keySplit = key.split("_")
                        val date = keySplit(0)
                        val province = keySplit(1)
                        val adid = keySplit(2).toLong

                        (date, province, adid, count)
                }

                import sparkSession.implicits._
                basicDateRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")

                val sql =
                    "select date, province, adid, count from(" +
                        "select date, province, adid, count," +
                        "row_number() over(partition by date, province order by count desc) rank from tmp_basic_info) t" +
                        "where rank <=3 "

                sparkSession.sql(sql).rdd
            }
        }

        top3DStream.foreachRDD {
            // 从SparkSql返回的是RDD[row]
            rdd => {

                // 遍历每个分区
                rdd.foreachPartition {
                    items =>
                        val top3Array = new ArrayBuffer[AdProvinceTop3]()

                        for (item <- items) {
                            val date = item.getAs[String]("date")
                            val province = item.getAs[String]("province")
                            val adid = item.getAs[Long]("adid")
                            val clickCount = item.getAs[Long]("count")

                            top3Array += AdProvinceTop3(date, province, adid, clickCount)
                        }

                        AdProvinceTop3DAO.updateBatch(top3Array.toArray)
                }
            }
        }
    }

    /**
      * 各省各城市一天中的广告点击量(累积统计)
      *
      * @param adRealTimeFilterDStream
      * @return
      */
    def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {

        // 将DStream中的RDD的数据进行转换为kv对
        // adRealTimeFilterDStream: DStream[RDD[String]]    String -> log : timestamp province city userid adid
        // key2ProvinceCityDStream: [RDD[(dateKey_province_city_adid, 1L)]]
        val key2ProvinceCityDStream = adRealTimeFilterDStream.map {

            //  log : timestamp province city userid adid
            case log =>
                val logSplit = log.split(" ")
                val timeStamp = logSplit(0).toLong

                // yy-mm-dd
                val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
                val province = logSplit(1)
                val city = logSplit(2)
                val adid = logSplit(4)

                val key = dateKey + "_" + province + "_" + city + "_" + adid

                (key, 1L)
        }

        // 有状态的累加
        // 某一天一个省的一个城市的某一个广告的累计数
        val key2StatDStream = key2ProvinceCityDStream.updateStateByKey[Long] {
            (values: Seq[Long], state: Option[Long]) =>

                // 当前值
                val currentCount = values.sum

                // 历史值
                val lastCount = state.getOrElse(0)

                // Scala里面最后一行代码就是返回值
                Some(currentCount + lastCount)

            /**
              * var currentCount = 0L
              *
              * if(state.isDefined){
              * val lastCount = state.get
              * }
              *
              * for ( value <- values){
              * currentCount += value
              * }
              *
              * Some(currentCount + lastCount)
              */
        }

        key2StatDStream.foreachRDD {
            rdd => {

                // 遍历每个分区
                rdd.foreachPartition {
                    items =>
                        val adStatArray = new ArrayBuffer[AdStat]()

                        for ((key, count) <- items) {
                            val keySplit = key.split("_")
                            val date = keySplit(0)
                            val province = keySplit(1)
                            val city = keySplit(2)
                            val adid = keySplit(3).toLong

                            adStatArray += AdStat(date, province, city, adid, count)
                        }

                        AdStatDAO.updateBatch(adStatArray.toArray)
                }
            }
        }

        key2StatDStream
    }

    /**
      * 需求一：实时维护黑名单
      *
      * @param adRealTimeFilterDStream
      */
    def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {

        // 将DStream中的RDD的数据进行转换为kv对
        // adRealTimeFilterDStream: DStream[RDD[String]]    String -> log : timestamp province city userid adid
        // key2NumDStream: [RDD[(dateKey_userId_adid, 1L)]]
        val key2NumDStream = adRealTimeFilterDStream.map {

            //  log : timestamp province city userid adid
            case log =>
                val logSplit = log.split(" ")
                val timeStamp = logSplit(0).toLong

                // yy-mm-dd
                val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
                val userId = logSplit(3).toLong
                val adid = logSplit(4).toLong

                val key = dateKey + "_" + userId + "_" + adid

                (key, 1L)
        }

        // 获取某一天某个用户某个广告 在当前RDD的计数
        val key2CountDStream = key2NumDStream.reduceByKey(_ + _)

        // 根据每一个RDD里面的数据，更新用户点击次数表
        // 遍历每个RDD
        key2CountDStream.foreachRDD {
            rdd =>

                // 遍历每个分区
                rdd.foreachPartition {
                    items =>
                        val clickCountArray = new ArrayBuffer[AdUserClickCount]()

                        for ((key, count) <- items) {
                            val keySplit = key.split("_")
                            val date = keySplit(0)
                            val userId = keySplit(1).toLong
                            val adid = keySplit(2).toLong

                            clickCountArray += AdUserClickCount(date, userId, adid, count)
                        }

                        AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
                }
        }

        // 对DStream执行filter操作,本质是对DStream中的RDD中每条数据执行filter操作
        // key2BlackListDStream: DStream[RDD[(key, count)]]
        val key2BlackListDStream = key2CountDStream.filter {
            case (key, count) =>
                val keySplit = key.split("_")
                val date = keySplit(0)
                val userId = keySplit(1).toLong
                val adid = keySplit(2).toLong

                val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)

                // 到达阈值的用户
                if (clickCount > 100) {
                    true
                } else {
                    false
                }
        }

        // key2BlackListDStream.map: DStream[RDD[userId]]
        val userIdDStream = key2BlackListDStream.map {
            case (key, count) => key.split("_")(1).toLong
        }.transform(rdd => rdd.distinct())

        userIdDStream.foreachRDD {
            rdd =>
                rdd.foreachPartition {
                    items =>
                        val userIdArray = new ArrayBuffer[AdBlacklist]()

                        for (userId <- items) {
                            userIdArray += AdBlacklist(userId)
                        }

                        AdBlacklistDAO.insertBatch(userIdArray.toArray)
                }
        }
    }
}
