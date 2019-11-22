package com.peng.state.state_backend;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求:
 * 每两条数据打印一次
 * <p>
 * 输入数据:
 * spark,3
 * hadoop,5
 * hadoop,7
 * flink,4
 * <p>
 * 打印结果:
 * (Spark,3) (Hadoop,5)
 * (Hadoop,7) (flink,4)
 * <p>
 * operatorState跟key没关系,每一个task中都有自己的operateState(计算状态)
 * 比如并行度是4,那么有4个task在运行,每个task都有自己的计算状态
 *
 * @author Administrator
 */
public class DoubleLinePrint {

    public static void main(String[] args) throws Exception {

        //准备上下文环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //这里设置并行度=1,方便查看结果
        //默认是根据cpu核数设置并行度
        //会扰乱数据输出
        env.setParallelism(1);

        /*
         * 1)存储在TaskManager的堆内存
         * 我们的状态数据存到哪儿是可以设置的，默认情况下
         * 默认情况下状态的数据就是存在TaskManager的堆内存里面
         * 也就是说如果程序挂了，那么我们的状态数据就丢失了
         */
        env.setStateBackend(new MemoryStateBackend());

        /*
         *  2)存储在HDFS上
         *  状态的数据会定期的被存储到持久化的文件系统上面
         *  我们的数据下一次重启的时候不会丢失了
         *
         *  存储/恢复这两个操作是不需要我们用户操作，是flink自动帮我们去搞的
         *
         *  注意flink中的state状态数据都是flink帮我们来处理,不需要手动操作
         *
         *  这种方式有可能丢数据
         *  因为数据是先存在JobManager的堆内存中,每一段时间同步到HDFS中
         *  如果堆内存比较小,数据比较大,就可能因为内存存不下过多的数据而丢弃
         *  这样,同步到HDFS上的数据是缺失的
         */
        //env.setStateBackend(new FsStateBackend("hdfs:/hadoop1:9000/xxx"));

        /*
         *  3)先存储在RocksDB上,再存储在HDFS上,不会丢数据
         *  RocksDB是一个分布式的数据库,可以帮我们存储数据,最后同步到HDFS上
         *  这样就不会丢失数据
         */
        //env.setStateBackend(new RocksDBStateBackend("hdfs:/hadoop1:9000/xxx"));

        //默认情况下我们是没有开启checkpoint的，如何开启checkpoint？
        //每隔10秒checkpoint一次，如果设置了这个参数就是开启了checkpoint
        //我们公司里面要 20秒 - 120秒
        env.enableCheckpointing(10000);

        //checkpoint可能会重复消费数据
        //因为checkpoint是有时间间隔的,如果挂了,那么在内存中的数据
        //还没有同步到HDFS中
        //下次重启的时候,从HDFS中拿到的数据还是上次的checkpoint
        //那么又要重新消费之前已经消费过的数据,所以会重复
        //这种问题可以用savepoint来解决

        //10:00:00 开始执行checkpoint
        //10:00:10 执行一次checkpoint需要时间，很有可能就是10秒也没有执行完
        //10.00:20 下一次执行checkpoint需要再过500毫秒才会执行
        //两次checkpoint之间最小的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        //默认支持的语义就是仅一次的语义
        //EXACTLY_ONCE  性能要差一小点
        //AT_LEAST_ONCE 性能好一些，数据就会有重复
        //这儿跟SparkStreaming是不一样的，SparkStreaming直接就是EXACTLY_ONCE，因为RDD的容错
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //checkpoint超时时间，如果当前的checkpoint一分钟都没有结束，那么就放弃
        //运行下一次checkpoint
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        //只保留最新的一个checkpoint的结果
        //如果设置了10，那么就会保留最新的10个checkpoint结果
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        /*
         * DELETE_ON_CANCELLATION(true) 停止任务的时候，删除checkpoint
         * RETAIN_ON_CANCELLATION(false) 停止任务的时候，保留checkpoint
         */
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //模拟测试数据
        DataStreamSource<Tuple2<String, Long>> dataStreamSource =
                env.fromElements(
                        Tuple2.of("Spark", 3L),
                        Tuple2.of("Hadoop", 5L),
                        Tuple2.of("Hadoop", 7L),
                        Tuple2.of("flink", 4L)
                );

        dataStreamSource.flatMap(new ListStateImp(2L)).print();

        env.execute("DoubleLinePrint");
    }
}
