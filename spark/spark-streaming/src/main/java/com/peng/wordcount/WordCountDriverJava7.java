package com.peng.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountDriverJava7 {
    public static void main(String[] args) throws Exception {
        //1.初始化程序入口
        SparkConf conf = new SparkConf();
        conf.setAppName("word count");
        conf.setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        //2.获取数据
        JavaReceiverInputDStream<String> dataStream = ssc.socketTextStream("localhost", 9999);
        //3.对数据进行业务处理
        //jdk1.7开发方式
        JavaDStream<String> wordDStream = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] fields = line.split(",");
                return Arrays.asList(fields).iterator();
            }
        });
        //k:word
        //value:1
        JavaPairDStream<String, Long> wordAndOneDStream = wordDStream.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String word) throws Exception {
                return new Tuple2<>(word, 1L);
            }
        });
        //按key累加
        JavaPairDStream<String, Long> result = wordAndOneDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        //4.数据输出
        result.print();
        //5.启动程序
        ssc.start();
        ssc.awaitTermination();
        ssc.stop();
    }
}
