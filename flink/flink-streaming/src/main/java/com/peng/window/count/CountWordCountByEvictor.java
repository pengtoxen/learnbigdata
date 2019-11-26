package com.peng.window.count;

import com.peng.window.CustomSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 使用Evictor自己实现一个类似CountWindow(3,2)的效果
 * 每隔2个单词计算最近3个单词
 *
 * @author Administrator
 */
public class CountWordCountByEvictor {

    public static void main(String[] args) throws Exception {

        //上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //自定义数据源
        DataStreamSource<String> dataStream = env.addSource(new CustomSource());

        //flatMap操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> keyedWindow = stream.keyBy(0)
                .window(GlobalWindows.create())
                .trigger(new MyCountTrigger(2))
                .evictor(new MyCountEvictor(3));


        DataStream<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);

        wordCounts.print().setParallelism(1);

        env.execute("Streaming WordCount");
    }


    private static class MyCountTrigger extends Trigger<Tuple2<String, Integer>, GlobalWindow> {

        //表示指定的元素的最大的数量
        private long maxCount;

        //用于存储每个 key 对应的 count 值
        private ReducingStateDescriptor<Long> stateDescriptor
                = new ReducingStateDescriptor<Long>("count", new ReduceFunction<Long>() {
            @Override
            public Long reduce(Long aLong, Long t1) throws Exception {
                return aLong + t1;
            }
        }, Long.class);

        MyCountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        /**
         * 当一个元素进入到一个 window 中的时候就会调用这个方法
         *
         * @param element   元素
         * @param timestamp 进来的时间
         * @param window    元素所属的窗口
         * @param ctx       上下文
         * @return TriggerResult
         * 1. TriggerResult.CONTINUE ：表示对 window 不做任何处理
         * 2. TriggerResult.FIRE ：表示触发 window 的计算
         * 3. TriggerResult.PURGE ：表示清除 window 中的所有数据
         * 4. TriggerResult.FIRE_AND_PURGE ：表示先触发 window 计算，然后删除 window 中的数据
         * @throws Exception
         */
        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element,
                                       long timestamp,
                                       GlobalWindow window,
                                       TriggerContext ctx) throws Exception {
            //拿到当前 key 对应的 count 状态值
            ReducingState<Long> count = ctx.getPartitionedState(stateDescriptor);
            //count 累加 1
            count.add(1L);
            //如果当前 key 的 count 值等于 maxCount
            if (count.get() == maxCount) {
                count.clear();
                //触发 window 计算，删除数据
                return TriggerResult.FIRE;
            }
            //否则，对 window 不做任何的处理
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            //写基于 Processing Time 的定时器任务逻辑
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            //写基于 Event Time 的定时器任务逻辑
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            //清除状态值
            ctx.getPartitionedState(stateDescriptor).clear();
        }
    }


    private static class MyCountEvictor implements Evictor<Tuple2<String, Integer>, GlobalWindow> {

        //window 的大小
        private long windowCount;

        MyCountEvictor(long windowCount) {
            this.windowCount = windowCount;
        }

        /**
         * 在 window 计算之前删除特定的数据
         *
         * @param elements       window 中所有的元素
         * @param size           window 中所有元素的大小
         * @param window         window
         * @param evictorContext 上下文
         */
        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            if (size <= windowCount) {
                return;
            } else {
                int evictorCount = 0;
                Iterator<TimestampedValue<Tuple2<String, Integer>>> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    evictorCount++;
                    //如果删除的数量小于当前的 window 大小减去规定的 window 的大小，就需要删除当前的元素
                    if (evictorCount > size - windowCount) {
                        break;
                    } else {
                        iterator.remove();
                    }
                }
            }
        }

        /**
         * 在 window 计算之后删除特定的数据
         *
         * @param elements       window 中所有的元素
         * @param size           window 中所有元素的大小
         * @param window         window
         * @param evictorContext 上下文
         */
        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
        }
    }
}
