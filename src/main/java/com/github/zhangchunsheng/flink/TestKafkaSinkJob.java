package com.github.zhangchunsheng.flink;

import com.github.zhangchunsheng.flink.model.MetricEvent;
import com.github.zhangchunsheng.flink.model.Student;
import com.github.zhangchunsheng.flink.schemas.MetricSchema;
import com.github.zhangchunsheng.flink.sink.SinkToMySQL1;
import com.github.zhangchunsheng.flink.utils.ExecutionEnvUtil;
import com.github.zhangchunsheng.flink.utils.GsonUtil;
import com.github.zhangchunsheng.flink.utils.KafkaConfigUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestKafkaSinkJob {
    public static void main(String[] args) throws Exception{
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);

        data.addSink(new FlinkKafkaProducer011<MetricEvent>(
                parameterTool.get("kafka.sink.brokers"),
                parameterTool.get("kafka.sink.topic"),
                new MetricSchema()
        )).name("flink-connectors-kafka")
                .setParallelism(parameterTool.getInt("stream.sink.parallelism"));

        env.execute("flink learning connectors kafka");
    }

    public static void main1(String[] args) throws Exception{
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
                .map(string -> GsonUtil.fromJson(string, Student.class)); //，解析字符串成 student 对象
    }

    public static void main2(String[] args) throws Exception{
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
                .map(string -> GsonUtil.fromJson(string, Student.class)); //
        student.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Student> values, Collector<List<Student>> out) throws Exception {
                ArrayList<Student> students = Lists.newArrayList(values);
                if (students.size() > 0) {
                    System.out.println("1 分钟内收集到 student 的数据条数是：" + students.size());
                    out.collect(students);
                }
            }
        }).addSink(new SinkToMySQL1());

        env.execute("flink learning connectors kafka");
    }
}
