package com.github.zhangchunsheng.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountExample {
    public static void main(String[] args) throws Exception {
        // 创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.使用StreamExecutionEnvironment创建DataStream
        //Source(可以有多个Source)
        //Socket 监听本地端口8888
        // 接收一个socket文本流
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        // Transformation(s) 对数据进行转换处理统计，先分词，再按照word进行分组，最后进行聚合统计
        DataStream<Tuple2<String, Integer>> windowCount = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    //将每个单词与 1 组合，形成一个元组
                    Tuple2<String, Integer> tp = Tuple2.of(word, 1);
                    //将组成的Tuple放入到 Collector 集合，并输出
                    collector.collect(tp);
                }
            }
        });

        // 1. 滚动窗口（Tumbling Windows）使用例子
        //进行分组聚合(keyBy：将key相同的分到一个组中) //定义一个1分钟的翻滚窗口,每分钟统计一次
        DataStream<Tuple2<String, Integer>> windowStream = windowCount.keyBy(0)
                .timeWindow(Time.minutes(1))
                .sum(1);

        // 调用Sink （Sink必须调用）
        windowStream.print("windows: ").setParallelism(1);
        //timePoint+=30;
        //启动(这个异常不建议try...catch... 捕获,因为它会抛给上层flink,flink根据异常来做相应的重启策略等处理)
        env.execute("StreamWordCount");
    }
}
