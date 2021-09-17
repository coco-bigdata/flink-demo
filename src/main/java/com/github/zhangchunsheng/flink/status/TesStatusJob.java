package com.github.zhangchunsheng.flink.status;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class TesStatusJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 注意设置 EventTime，而不是默认的 ProcessTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator dataStream = env.addSource(new ReadLineSource("src/main/resources/data.txt"))
                // 注意 FlatMapFunction 不要写成 Lambda 表达式
                // 我们使用了泛型，所以没有显式地指明返回值的类型的话会出错
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> out) {
                        String[] split = s.split(",");
                        if ("pv".equals(split[3])) {
                            Tuple2 res = new Tuple2<>(split[0] + "-" + split[1], Long.parseLong(split[4]));
                            out.collect(res);
                        }
                    }
                });
    }
}
