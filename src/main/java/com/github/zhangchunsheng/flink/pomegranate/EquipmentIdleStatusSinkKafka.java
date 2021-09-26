package com.github.zhangchunsheng.flink.pomegranate;

import com.github.zhangchunsheng.flink.model.CUnpackDataT;
import com.github.zhangchunsheng.flink.schemas.CUnpackDataTSchema;
import com.github.zhangchunsheng.flink.utils.DateUtil;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class EquipmentIdleStatusSinkKafka {
    private final static Gson gson = new Gson();
    private final static String SOURCE_TOPIC = "c_unpack_data_t_topic";
    private final static String SINK_TOPIC = "c_equipment_idle_status_topic";
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public static void main(String[] args) throws Exception {

        // 1 设置环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 定义数据
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.200:9092,192.168.0.160:9092,192.168.0.178:9092");
        props.put("zookeeper.connect", "192.168.0.200:2181,192.168.0.160:2181,192.168.0.178:2181");
        props.put("group.id", "equipment-status-group-2");
        // props.put("deserializer.encoding", "utf-8");
        //props.put("acks", "all");
        props.put("retries", 0);
        // 同时设置batch.size和linger.ms,就是哪个条件先满足就都会将消息发送出去
        // props.put("batch.size", 16384);
        // props.put("linger.ms", 10);
        // 一次调用poll()操作时返回的最大记录数，默认值为500
        props.put("max.poll.records", "10000");
        // props.put("buffer.memory", 33554432);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        System.out.println("Time: " + DateUtil.getTime());

        DataStreamSource<String> text = env.addSource(new FlinkKafkaConsumer011<>(
                SOURCE_TOPIC,
                new SimpleStringSchema(),
                props));

        // 3. 处理逻辑
        DataStream<Tuple2<String, CUnpackDataT>> counts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Map<String, String>>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
                if (StringUtils.isNullOrWhitespaceOnly(value)) {
                    return;
                }
                //解析message中的json
                Map<String, String> map = gson.fromJson(value, new TypeToken<Map<String, String>>() {
                }.getType());
                String equipmentNumber = map.getOrDefault("equipment_number", "");
                out.collect(new Tuple2<>(equipmentNumber, map));
            }
        })

                .keyBy(value -> value.f0)
                .flatMap(new RichFlatMapFunction<Tuple2<String, Map<String, String>>, Tuple2<String, CUnpackDataT>>() {
                    //保存处理状态
                    ValueState<String> isProcess = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastStatusDescriptor = new ValueStateDescriptor<>("isProcess", String.class);
                        isProcess = getRuntimeContext().getState(lastStatusDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<String, Map<String, String>> in, Collector<Tuple2<String, CUnpackDataT>> out) throws Exception {
                        if(isProcess == null || isProcess.value() == null) {
                            // process
                            isProcess.update("1");
                        } else {
                            return;
                        }

                        CUnpackDataT cUnpackDataT;
                        String equipmentNumber = in.f1.get("equipment_number");
                        String collectEmpStatus = "-2";
                        String day;
                        while(true) {
                            cUnpackDataT = new CUnpackDataT();
                            day = DateUtil.getDay();
                            cUnpackDataT.setPackageTime(System.currentTimeMillis());
                            cUnpackDataT.setEquipmentNumber(equipmentNumber);
                            cUnpackDataT.setIp(in.f1.get("ip"));
                            cUnpackDataT.setStatus(Integer.valueOf(collectEmpStatus));
                            cUnpackDataT.setPackageDate(Integer.valueOf(day));

                            cUnpackDataT.setPackageNo(Integer.valueOf(in.f1.get("package_no")));
                            cUnpackDataT.setWorkTime(Long.valueOf(in.f1.get("work_time")));
                            cUnpackDataT.setStandbyTime(Long.valueOf(in.f1.get("standby_time")));
                            cUnpackDataT.setWarningTime(Long.valueOf(in.f1.get("warning_time")));
                            cUnpackDataT.setPieceCnt(Integer.valueOf(in.f1.get("piece_cnt")));

                            out.collect(new Tuple2<>(equipmentNumber + ":" + collectEmpStatus, cUnpackDataT));
                        }
                    }
                })
                .keyBy(v -> v.f0);

        // 4. 打印结果
        counts.addSink(new FlinkKafkaProducer010<Tuple2<String, CUnpackDataT>>(
                "192.168.0.200:9092,192.168.0.160:9092,192.168.0.178:9092", SINK_TOPIC,
                new CUnpackDataTSchema()
        ));

        // execute program
        env.execute("Equipment idle status");

    }
}
