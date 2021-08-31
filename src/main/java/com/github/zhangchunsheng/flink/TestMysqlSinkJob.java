package com.github.zhangchunsheng.flink;

import com.alibaba.fastjson.JSON;
import com.github.zhangchunsheng.flink.model.Student;
import com.github.zhangchunsheng.flink.sink.PrintSinkFunction;
import com.github.zhangchunsheng.flink.sink.SinkToMySQL;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class TestMysqlSinkJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                "student",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> JSON.parseObject(string, Student.class)); //Fastjson 解析字符串成 student 对象

        student.addSink(new SinkToMySQL()); //数据 sink 到 mysql
        // student.addSink(new PrintSinkFunction<>());

        SingleOutputStreamOperator<Student> map = student.map(new MapFunction<Student, Student>() {
            @Override
            public Student map(Student value) throws Exception {
                Student s1 = new Student();
                s1.studentId = value.studentId;
                s1.name = value.name;
                s1.password = value.password;
                s1.age = value.age + 5;
                return s1;
            }
        });
        map.print();

        SingleOutputStreamOperator<Student> flatMap = student.flatMap(new FlatMapFunction<Student, Student>() {
            @Override
            public void flatMap(Student value, Collector<Student> out) throws Exception {
                if (value.studentId % 2 == 0) {
                    out.collect(value);
                }
            }
        });
        flatMap.print();

        SingleOutputStreamOperator<Student> filter = student.filter(new FilterFunction<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                if (value.studentId > 95) {
                    return true;
                }
                return false;
            }
        });
        filter.print();

        KeyedStream<Student, Integer> keyBy = student.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return value.age;
            }
        });
        keyBy.print();

        SingleOutputStreamOperator<Student> reduce = student.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return value.age;
            }
        }).reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student value1, Student value2) throws Exception {
                Student student1 = new Student();
                student1.name = value1.name + value2.name;
                student1.studentId = (value1.studentId + value2.studentId) / 2;
                student1.password = value1.password + value2.password;
                student1.age = (value1.age + value2.age) / 2;
                return student1;
            }
        });
        reduce.print();

        /*KeyedStream.fold("1", new FoldFunction<Integer, String>() {
            @Override
            public String fold(String accumulator, Integer value) throws Exception {
                return accumulator + "=" + value;
            }
        });

        KeyedStream.sum(0)
        KeyedStream.sum("key")
        KeyedStream.min(0)
        KeyedStream.min("key")
        KeyedStream.max(0)
        KeyedStream.max("key")
        KeyedStream.minBy(0)
        KeyedStream.minBy("key")
        KeyedStream.maxBy(0)
        KeyedStream.maxBy("key")
        */

        // inputStream.keyBy(0).window(Time.seconds(10));

        // inputStream.keyBy(0).windowAll(Time.seconds(10));

        env.execute("Flink add sink");
    }
}
