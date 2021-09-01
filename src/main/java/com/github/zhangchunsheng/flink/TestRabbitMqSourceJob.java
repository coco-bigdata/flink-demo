package com.github.zhangchunsheng.flink;

import com.github.zhangchunsheng.flink.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.stream.Stream;

/**
 * 从 rabbitmq 读取数据
 */
public class TestRabbitMqSourceJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;

        //这些配置建议可以放在配置文件中，然后通过 parameterTool 来获取对应的参数值
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig
                .Builder().setHost("localhost").setVirtualHost("/")
                .setPort(5672).setUserName("admin").setPassword("admin")
                .build();

        DataStreamSource<String> peter = env.addSource(new RMQSource<>(connectionConfig,
                "peter",
                true,
                new SimpleStringSchema()))
                .setParallelism(1);
        peter.print();

        //如果想保证 exactly-once 或 at-least-once 需要把 checkpoint 开启
//        env.enableCheckpointing(10000);
        env.execute("flink learning connectors rabbitmq");

        //读取电影列表数据集合
        /*DataSet<Tuple3<Long, String, String>> lines = env.readCsvFile("movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        lines.filter((FilterFunction<Tuple3<Long, String, String>>) movie -> {
            // 以“|”符号分隔电影类型
            String[] genres = movie.f2.split("\\|");

            // 查找所有 “动作” 类型的电影
            return Stream.of(genres).anyMatch(g -> g.equals("Action"));
        }).print();*/
    }
}