package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author Administrator
 */
public class ProducerDemo {
    public static void main(String[] args) throws Exception {
        /*
         * 步骤一：设置参数
         */
        //创建了一个配置文件的对象
        Properties props = new Properties();
        //这个参数，目的就是为了获取kafka集群的元数据
        //我们写一台主机也行，写多个的话，更安全。
        //大家注意到，我这儿使用的是主机名。原因是server.properties 的时候里面填进去的
        //就是主机名。所以这儿必须是主机名。既然是主机名的话，所以你必须要配置你的电脑的
        //hosts文件。
        //这个问题，很多同学都会遇到。
        props.put("bootstrap.servers", "hadoop1:9092,hadoop2:9092,hadoop3:9092");
        //设置序列化器 -》 kafka的数据是用的网络传输的，所以里面都是二进制的数据。
        //我们发送消息的时候，默认的情况下就是发送一个消息就可以了。，
        //但是你也可以给你的每条消息都指定一个key也是可以的。
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /*
         * acks：
         *  -1
         *  1
         *  0
         *
         * 参数为-1：
         *  如何判断一条消息发送成功呢？首先消息要写入leader partition，这些消息还需要被另外的所有的这个
         *  分区的副本同步了 才算发送成功。
         *
         * Kafka里面的分区是有副本的。比如有一个主题topic
         * 这个主题里面有2个 partition，每个分区有3个副本
         * p0:
         *   leader partition,follower partition,follower partition
         * p1:
         *   leader partition,follower partition,follower partition
         *
         * 如果参数是1：
         *  重要发送的消息能写到leader partition就算写入成功，然后服务端就返回响应就可以了
         *  默认就是用的这个参数
         *  有可能会丢数据。
         *  消息  -》 leader partition -> 响应消息 得到的就是发送成功 -》 用户是不是就认为发送成功了。
         *  刚好这个leader partition宕机了。
         *
         * 如果参数是0：
         *  消息只要发送出去了，那么就认为是成功的，不管了。
         *  我们可能允许丢数据，只是在处理一些不重要的日志，分析里面一些简单的数据
         *  不需要等到准确的数据。
         */
        props.put("acks", "-1");
        /*
         * 这个参数重要，大家在生产环境里面一定要设置
         * 重试几次。
         * 告诉大家一个经验值，如果我们设置了重试这个参数以后，95%的异常都可以搞定。
         * 5-10次
         */
        props.put("retries", 3);

        //每隔多久重试一次 2000ms
        props.put("retry.backoff.ms", 2000);

        //如果我想提升吞吐量,压缩格式
        props.put("compression.type", "lz4");
        /*
         * 缓冲区的大小，默认是32M，不过我告诉大家，
         * 基本上32M是合理的，没问题的。64M
         * 但是我告诉大家，你知道原理就可以了。
         * 基本上我们不需要设置的。
         * 33554432 byte = 32M
         */
        props.put("buffer.memory", 33554432);
        /*
         * 批次的大小
         * 默认是16k  -> 32K
         * 设置这个批次的大小 还跟我们的消息的大小的有关。
         * 假设我们的一条消息的大小是：16K
         * 这样的话，你的这个批次的意义还有吗？没有了！
         * 假设你的一条消息的大小如果是1K  -》100K
         *
         * 这两个参数开发的时候需要去配置的。
         */
        props.put("batch.size", 323840);
        /*
         * 比如我们设置的一个批次的大小是32K。
         * 但是我们现在一个消息的大小是1k,现在这个批次里面已经有了
         * 3条数据，3k
         * 即使一个批次没满，无论如何到了这个时间都要把消息发送出去了。
         */
        props.put("linger.ms", 100);
        /*
         * 这个值，默认是1M
         * 代表的是生产者发送消息的时候，最大的一条信息（注意说的不是批次）
         * 最大能有多大，默认是1M ，也就是说，如果有消息超过了1M，
         * 程序会报错。所以这个值建议一定要设置
         * 设置成为10M
         * 后时候打印日志的时候，会把多条消息合并成为了一条消息。
         *
         * byte
         * 1024 * 1024  = 1M
         * 10 * 1024 * 1024
         */
        props.put("max.request.size", 1048576);
        /*
         * 消息发送出去了以后，多久了还是没接收到响应，那么就认为超时。
         * 如果发现自己公司，经常出现网络不稳定的情况
         * 大家可以根据自己的情况，适当可以把这个值调大一点。
         */
        props.put("request.timeout.ms", 3000);

        /*
         * 步骤二：创建生产者
         */
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        /*
         * 步骤三：创建消息
         *
         * 在kafka里面，我们发送消息的时候，可以给消息指定key,也可以不指定。
         *  key跟我们要把这个消息发送到这个主题的哪个分区有关系
         *  比如：peng:
         *          p0:
         *              leader partition    <-  ,follower partition
         *          p1:
         *              leader partition   <-  ,follower partition
         * （1）不指定key
         *          发送的一条消息，会以轮询的方式 发送到分区里面。(要么就是轮询，要么随机)
         *          Hadoop   p0
         *          Flink    p1
         *          Hbase    p0
         *          hadoop   p1
         * （2）如果指定key
         *       test 取这个key的hash值   数字 3
         *       数字/分区数
         *       3/2
         *
         *       比如我们分区数2，结果要么就是0 要么就是1
         *       test，Hadoop  -》  p1
         *       这样子的话，我们可以保证这样的一个事，key相同的消息
         *       一定会被发送到同一个分区。
         *
         */
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("peng", "test", "hadoop");

        /*
         * 步骤四：发送消息
         *
         * 发送消息有两种方式：
         * （1）异步发送
         *      性能比较好，也是我们生产里面使用的方式
         * （2）同步发送
         *      这种方式，性能不好，我们生产里面一般不用
         *      但是从我们测试的时候是可以使用的。
         */

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                /*
                 * 假设我们设置了重试3次，重试了3以后都不行，才会给我们返回异常。
                 * 如果重试到第三次，就OK了，那么这儿最后给我们返回来的
                 * 结果是executive 不等于 null
                 */
                if (exception == null) {
                    System.out.println("消息发送成功");
                } else {
                    //超时的异常。
                    System.out.println("做其他处理");
                }
            }

        });
        Thread.sleep(100000);
        // 这是同步发送的模式 try
        // producer.send(record).get();
        // 你要一直等待人家后续一系列的步骤都做完，发送消息之后
        // 有了消息的回应返回给你，你这个方法才会退出来

        //步骤五：关闭链接
        producer.close();
    }
}

