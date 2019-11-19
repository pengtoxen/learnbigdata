package com.peng.source.split_stream;

import com.peng.source.no_parallelism.MyNoParallelismSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 根据规则把一个数据流切分为多个流
 * 应用场景：
 * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以在根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 *
 * @author Administrator
 */
public class SplitStream {

    public static void main(String[] args) throws Exception {

        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        //注意：针对此source，并行度只能设置为1
        DataStreamSource<Long> text = env.addSource(new MyNoParallelismSource()).setParallelism(1);
        //对流进行切分，按照数据的奇偶性进行区分
        org.apache.flink.streaming.api.datastream.SplitStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPut = new ArrayList<>();
                if (value % 2 == 0) {
                    //偶数
                    //内部做了处理,even是区分的流的标识
                    outPut.add("even");
                } else {
                    //奇数
                    //内部做了处理,odd是区分的流的标识
                    outPut.add("odd");
                }
                return outPut;
            }
        });

        //选择一个或者多个切分后的流
        //因为做了区分,所以可以通过select方法选择流
        DataStream<Long> evenStream = splitStream.select("even");
        DataStream<Long> oddStream = splitStream.select("odd");
        DataStream<Long> moreStream = splitStream.select("odd", "even");

        //打印结果
        oddStream.print().setParallelism(1);
        String jobName = SplitStream.class.getSimpleName();
        env.execute(jobName);
    }
}