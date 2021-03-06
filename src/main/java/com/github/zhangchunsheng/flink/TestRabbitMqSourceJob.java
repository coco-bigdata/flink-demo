package com.github.zhangchunsheng.flink;

import com.github.zhangchunsheng.flink.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.Collection;
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

        // 传递类型名称
        /*lines.filter(new FilterGenre("Action"))
                .print();*/

        /*final String genre = "Action";

        lines.filter((FilterFunction<Tuple3<Long, String, String>>) movie -> {
            String[] genres = movie.f2.split("\\|");

            //使用变量
            return Stream.of(genres).anyMatch(g -> g.equals(genre));
        }).print();*/

        // Configuration 类来存储参数
        /*Configuration configuration = new Configuration();
        configuration.setString("genre", "Action");

        lines.filter(new FilterGenreWithParameters())
                // 将参数传递给函数
                .withParameters(configuration)
                .print();*/

        //读取命令行参数
        ParameterTool parameterTool1 = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool1);

        //该函数将能够读取这些全局参数
        /*lines.filter(new FilterGenreWithGlobalEnv()) //这个函数是自己定义的
                .print();*/

        /*DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);
        // 获取要忽略的单词集合
        DataSet<String> wordsToIgnore = ...

        data.map(new RichFlatMapFunction<String, String>() {

            // 存储要忽略的单词集合. 这将存储在 TaskManager 的内存中
            Collection<String> wordsToIgnore;

            @Override
            public void open(Configuration parameters) throws Exception {
                //读取要忽略的单词的集合
                wordsToIgnore = getRuntimeContext().getBroadcastVariable("wordsToIgnore");
            }

            @Override
            public String map(String line, Collector<String> out) throws Exception {
                String[] words = line.split("\\W+");
                for (String word : words)
                    //使用要忽略的单词集合
                    if (wordsToIgnore.contains(word))
                        out.collect(new Tuple2<>(word, 1));
            }
            //通过广播变量传递数据集
        }).withBroadcastSet(wordsToIgnore, "wordsToIgnore");*/

        //从 HDFS 注册文件
        env.registerCachedFile("hdfs:///path/to/file", "machineLearningModel");

        env.execute();

        //要处理的数据集合
        /*DataSet<String> lines;

        // Word count 算法
        lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split("\\W+");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        })
                .groupBy(0)
                .sum(1)
                .print();

        // 计算要处理的文本中的行数
        long linesCount = lines.count();
        System.out.println(linesCount);*/

        /*lines.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

            //创建一个累加器
            private IntCounter linesNum = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                //注册一个累加器
                getRuntimeContext().addAccumulator("linesNum", linesNum);
            }

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split("\\W+");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }

                // 处理每一行数据后 linesNum 递增
                linesNum.add(1);
            }
        })
                .groupBy(0)
                .sum(1)
                .print();

        //获取累加器结果
        int linesNum = env.getLastJobExecutionResult().getAccumulatorResult("linesNum");
        System.out.println(linesNum);*/
    }

    static class FilterGenre implements FilterFunction<Tuple3<Long, String, String>> {
        //类型
        String genre;
        //初始化构造方法
        public FilterGenre(String genre) {
            this.genre = genre;
        }

        @Override
        public boolean filter(Tuple3<Long, String, String> movie) throws Exception {
            String[] genres = movie.f2.split("\\|");

            return Stream.of(genres).anyMatch(g -> g.equals(genre));
        }
    }

    static class FilterGenreWithParameters extends RichFilterFunction<Tuple3<Long, String, String>> {
        String genre;

        @Override
        public void open(Configuration parameters) throws Exception {
            //读取配置
            genre = parameters.getString("genre", "");
        }

        @Override
        public boolean filter(Tuple3<Long, String, String> movie) throws Exception {
            String[] genres = movie.f2.split("\\|");

            return Stream.of(genres).anyMatch(g -> g.equals(genre));
        }
    }

    static class FilterGenreWithGlobalEnv extends RichFilterFunction<Tuple3<Long, String, String>> {
        @Override
        public boolean filter(Tuple3<Long, String, String> movie) throws Exception {
            String[] genres = movie.f2.split("\\|");
            //获取全局的配置
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            //读取配置
            String genre = parameterTool.get("genre");

            return Stream.of(genres).anyMatch(g -> g.equals(genre));
        }
    }

    static class MyClassifier extends RichMapFunction<String, Integer> {
        @Override
        public void open(Configuration config) {
            File machineLearningModel = getRuntimeContext().getDistributedCache().getFile("machineLearningModel");
        }

        @Override
        public Integer map(String value) throws Exception {
            return 0;
        }
    }
}
