package com.peng.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountDriverJava8 {
    public static void main(String[] args) throws Exception {
        //1.初始化程序入口
        SparkConf conf = new SparkConf();
        conf.setAppName("word count");
        conf.setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        //2.获取数据
        JavaReceiverInputDStream<String> dataStream = ssc.socketTextStream("localhost", 9999);
        //3.对数据进行业务处理
        JavaPairDStream<String, Long> result = dataStream.flatMap(line -> Arrays.asList(line.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((v1, v2) -> v1 + v2);
        //4.数据输出
        result.print();
        //5.启动程序
        ssc.start();
        ssc.awaitTermination();
        ssc.stop();
    }
}
