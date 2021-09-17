package com.github.zhangchunsheng.flink.status;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;

public class KafkaKeyedStateSample {
    private final static Gson gson = new Gson();
    private final static String SOURCE_TOPIC = "test5";
    private final static String SINK_TOPIC = "test6";
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public static void main(String[] args) throws Exception {

        // 1 设置环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 定义数据
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "test-read-group-4");
        props.put("deserializer.encoding", "GB2312");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> text = env.addSource(new FlinkKafkaConsumer011<>(
                SOURCE_TOPIC,
                new SimpleStringSchema(),
                props));

        // 3. 处理逻辑
        DataStream<Tuple2<String, Long>> counts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Map<String, String>>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
                if (StringUtils.isNullOrWhitespaceOnly(value)) {
                    return;
                }
                //解析message中的json
                Map<String, String> map = gson.fromJson(value, new TypeToken<Map<String, String>>() {
                }.getType());
                String employee = map.getOrDefault("employee", "");
                out.collect(new Tuple2<>(employee, map));
            }
        })

                .keyBy(value -> value.f0)
                .flatMap(new RichFlatMapFunction<Tuple2<String, Map<String, String>>, Tuple2<String, Long>>() {
                    //保存最后1次上报状态的时间戳
                    ValueState<Long> lastTimestamp = null;
                    //保存最后1次的状态
                    ValueState<String> lastStatus = null;
                    //记录每个状态的持续时长累加值
                    MapState<String, Long> statusDuration = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Long> lastTimestampDescriptor = new ValueStateDescriptor<>("lastTimestamp", Long.class);
                        lastTimestamp = getRuntimeContext().getState(lastTimestampDescriptor);

                        ValueStateDescriptor<String> lastStatusDescriptor = new ValueStateDescriptor<>("lastStatus", String.class);
                        lastStatus = getRuntimeContext().getState(lastStatusDescriptor);

                        MapStateDescriptor<String, Long> statusDurationDescriptor = new MapStateDescriptor<>("statusDuration", String.class, Long.class);
                        statusDuration = getRuntimeContext().getMapState(statusDurationDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<String, Map<String, String>> in, Collector<Tuple2<String, Long>> out) throws Exception {
                        long timestamp = Long.parseLong(in.f1.get("event_timestamp"));
                        String employee = in.f1.get("employee");
                        String empStatus = in.f1.get("status");
                        String collectEmpStatus = empStatus;
                        long duration = 0;
                        if (lastTimestamp == null || lastTimestamp.value() == null) {
                            //第1条数据
                            duration = 0;
                        } else if (timestamp > lastTimestamp.value()) { //不接受乱序数据
                            if (empStatus.equalsIgnoreCase(lastStatus.value())) {
                                //状态没变，时长累加
                                duration = statusDuration.get(collectEmpStatus) + (timestamp - lastTimestamp.value());
                            } else {
                                //状态变了,上次的状态时长累加
                                // timestamp statusDuration lastTimestamp equipment package_date status
                                collectEmpStatus = lastStatus.value();
                                duration = statusDuration.get(collectEmpStatus) + (timestamp - lastTimestamp.value());
                            }
                        } else {
                            return;
                        }
                        lastTimestamp.update(timestamp);
                        lastStatus.update(empStatus);
                        statusDuration.put(collectEmpStatus, duration);
                        if (!collectEmpStatus.equalsIgnoreCase(empStatus) && !statusDuration.contains(empStatus)) {
                            statusDuration.put(empStatus, 0L);
                        }
                        out.collect(new Tuple2<>(employee + ":" + collectEmpStatus, duration));
                    }
                })
                .keyBy(v -> v.f0);

        // 4. 打印结果
        counts.addSink(new FlinkKafkaProducer010<>("localhost:9092", SINK_TOPIC,
                (SerializationSchema<Tuple2<String, Long>>) element -> ("(" + element.f0 + "," + element.f1 + ")").getBytes()));
        counts.print();

        // execute program
        env.execute("Kafka Streaming KeyedState sample");

    }
}
