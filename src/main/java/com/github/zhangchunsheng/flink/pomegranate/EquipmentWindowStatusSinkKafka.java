package com.github.zhangchunsheng.flink.pomegranate;

import com.alibaba.fastjson.JSONObject;
import com.github.zhangchunsheng.flink.model.EquipmentWorkTime;
import com.github.zhangchunsheng.flink.schemas.EquipmentWorkTimeSchema1;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.awt.*;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class EquipmentWindowStatusSinkKafka {
    private final static Gson gson = new Gson();
    private final static String SOURCE_TOPIC = "c_unpack_data_t_topic1";
    private final static String SINK_TOPIC = "c_equipment_status_topic";
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public static void main(String[] args) throws Exception {

        // 1 ????????????
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. ????????????
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.200:9092,192.168.0.160:9092,192.168.0.178:9092");
        props.put("zookeeper.connect", "192.168.0.200:2181,192.168.0.160:2181,192.168.0.178:2181");
        props.put("group.id", "equipment-status-group-1");
        // props.put("deserializer.encoding", "utf-8");
        //props.put("acks", "all");
        props.put("retries", 0);
        // ????????????batch.size???linger.ms,?????????????????????????????????????????????????????????
        // props.put("batch.size", 16384);
        // props.put("linger.ms", 10);
        // ????????????poll()????????????????????????????????????????????????500
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

        // 3. ????????????
        DataStream<Tuple2<String, EquipmentWorkTime>> counts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Map<String, String>>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
                if (StringUtils.isNullOrWhitespaceOnly(value)) {
                    return;
                }
                //??????message??????json
                Map<String, String> map = gson.fromJson(value, new TypeToken<Map<String, String>>() {
                }.getType());
                String equipmentNumber = map.getOrDefault("equipment_number", "");
                out.collect(new Tuple2<>(equipmentNumber, map));
            }
        })

                .keyBy(value -> value.f0)
                .flatMap(new RichFlatMapFunction<Tuple2<String, Map<String, String>>, Tuple2<String, EquipmentWorkTime>>() {
                    //????????????1???????????????????????????
                    ValueState<Long> lastPackageTime = null;
                    //?????????1???????????????????????????
                    ValueState<Long> startPackageTime = null;
                    //????????????1????????????????????????
                    ValueState<String> lastPackageDate = null;
                    //????????????1????????????
                    ValueState<String> lastStatus = null;
                    //??????????????????????????????????????????
                    MapState<String, Long> statusDuration = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Long> lastPackageTimeDescriptor = new ValueStateDescriptor<>("lastPackageTime", Long.class);
                        lastPackageTime = getRuntimeContext().getState(lastPackageTimeDescriptor);

                        ValueStateDescriptor<Long> startPackageTimeDescriptor = new ValueStateDescriptor<>("startPackageTime", Long.class);
                        startPackageTime = getRuntimeContext().getState(startPackageTimeDescriptor);

                        ValueStateDescriptor<String> lastPackageDateDescriptor = new ValueStateDescriptor<>("lastPackageDate", String.class);
                        lastPackageDate = getRuntimeContext().getState(lastPackageDateDescriptor);

                        ValueStateDescriptor<String> lastStatusDescriptor = new ValueStateDescriptor<>("lastStatus", String.class);
                        lastStatus = getRuntimeContext().getState(lastStatusDescriptor);

                        MapStateDescriptor<String, Long> statusDurationDescriptor = new MapStateDescriptor<>("statusDuration", String.class, Long.class);
                        statusDuration = getRuntimeContext().getMapState(statusDurationDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<String, Map<String, String>> in, Collector<Tuple2<String, EquipmentWorkTime>> out) throws Exception {
                        long packageTime = Long.parseLong(in.f1.get("package_time"));
                        String equipmentNumber = in.f1.get("equipment_number");
                        String empStatus = in.f1.get("status");
                        String packageDate = in.f1.get("package_date");
                        String collectEmpStatus = empStatus;
                        Long duration = 0L;
                        boolean isChanged = false;
                        String day = DateUtil.getDay();

                        if(!day.equals(packageDate)) { //?????????????????????
                            return;
                        }
                        if(empStatus.equals("-2")) { //?????????????????????
                            return;
                        }
                        if(lastPackageDate == null || lastPackageDate.value() == null) {
                            lastPackageDate.update(packageDate);
                        }

                        if (lastPackageTime == null || lastPackageTime.value() == null) {
                            //???1?????????
                            duration = 0L;
                        } else if (packageTime > lastPackageTime.value()) { //?????????????????????
                            if (empStatus.equalsIgnoreCase(lastStatus.value())) {
                                if(!day.equals(lastPackageDate.value())) { //need init data
                                    collectEmpStatus = lastStatus.value();
                                    isChanged = true;
                                    duration = statusDuration.get(collectEmpStatus) + (packageTime - lastPackageTime.value());
                                } else {
                                    //???????????????????????????
                                    duration = statusDuration.get(collectEmpStatus) + (packageTime - lastPackageTime.value());
                                }
                            } else {
                                //????????????,???????????????????????????
                                // packageTime statusDuration lastPackageTime equipment package_date status
                                collectEmpStatus = lastStatus.value();
                                isChanged = true;
                                duration = statusDuration.get(collectEmpStatus) + (packageTime - lastPackageTime.value());
                            }
                        } else {
                            return;
                        }
                        EquipmentWorkTime equipmentWorkTime = new EquipmentWorkTime();
                        if (startPackageTime == null || startPackageTime.value() == null) {
                            equipmentWorkTime.setStartPackageTime(packageTime);
                            startPackageTime.update(packageTime);
                        } else {
                            equipmentWorkTime.setStartPackageTime(startPackageTime.value());
                        }
                        if(isChanged) {
                            startPackageTime.update(packageTime);
                        }
                        equipmentWorkTime.setEndPackageTime(packageTime);
                        equipmentWorkTime.setStatusDuration(duration.intValue());
                        equipmentWorkTime.setEquipmentNumber(equipmentNumber);
                        equipmentWorkTime.setIp(in.f1.get("ip"));

                        double durationMinute = equipmentWorkTime.getStatusDuration().doubleValue() / 1000 / 60;
                        equipmentWorkTime.setStatus(Integer.valueOf(collectEmpStatus));
                        equipmentWorkTime.setDurationMinute(durationMinute);
                        equipmentWorkTime.setPackageDate(Integer.valueOf(lastPackageDate.value()));
                        equipmentWorkTime.setPackageNo(Integer.valueOf(in.f1.get("package_no")));
                        equipmentWorkTime.setWorkTime(Long.valueOf(in.f1.get("work_time")));
                        equipmentWorkTime.setStandbyTime(Long.valueOf(in.f1.get("standby_time")));

                        equipmentWorkTime.setCount(1);

                        equipmentWorkTime.setWarningTime(Long.valueOf(in.f1.get("warning_time")));
                        equipmentWorkTime.setPieceCnt(Integer.valueOf(in.f1.get("piece_cnt")));

                        if(!day.equals(lastPackageDate.value())) { //init data
                            duration = 0L;
                            // all statusDuration
                            Iterator<Map.Entry<String, Long>> iterator = statusDuration.iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<String, Long> entry = iterator.next();
                                statusDuration.put(entry.getKey(), duration);
                            }
                            lastPackageDate.update(packageDate);
                        }

                        lastPackageTime.update(packageTime);
                        lastStatus.update(empStatus);
                        statusDuration.put(collectEmpStatus, duration);
                        /*if (!collectEmpStatus.equalsIgnoreCase(empStatus) && !statusDuration.contains(empStatus)) {
                            statusDuration.put(empStatus, 0L);
                        }*/
                        if (!collectEmpStatus.equalsIgnoreCase(empStatus)) {
                            statusDuration.put(empStatus, 0L);
                        }
                        out.collect(new Tuple2<>(equipmentNumber + ":" + collectEmpStatus, equipmentWorkTime));
                    }
                })
                .keyBy(v -> v.f0);
                // .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))

        // 4. ????????????
        counts.addSink(new FlinkKafkaProducer010<Tuple2<String, EquipmentWorkTime>>(
                "192.168.0.200:9092,192.168.0.160:9092,192.168.0.178:9092", SINK_TOPIC,
                new EquipmentWorkTimeSchema1()
        ));
        // to mysql
        // counts.addSink(new SinkWorkTimeToMySQL());
        // counts.print();

        // execute program
        env.execute("Equipment status statistics");
    }

    static void halfHourNonPaid(SingleOutputStreamOperator<JSONObject> outputStream, OutputTag<JSONObject> ordersHalfhourPaid) {
        outputStream.getSideOutput(ordersHalfhourPaid)
                .map(value -> {
                    return new PaidWarn(value.containsKey("really_data"), 1, Long.parseLong(value.getOrDefault("", "").toString())
                    , value.containsKey("order_id") ? value.getString("order_id") : "");
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<PaidWarn>() {
                    long currentTimestamp = Long.MIN_VALUE;
                    final long maxTimeLag = 5000;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
                    }

                    @Override
                    public long extractTimestamp(PaidWarn element, long previousElementTimestamp) {
                        long timestamp = element.getPaidCreateTime();
                        currentTimestamp = Math.max(timestamp, currentTimestamp);
                        return timestamp;
                    }
                })
                //.keyBy(PaidWarn::getKey)
                //.process(new PaidWarningProcessFunction(1000 * 60 * 10))
                .timeWindowAll(Time.minutes(30))
                .apply(new AllWindowFunction<PaidWarn, Integer, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<PaidWarn> values, Collector<Integer> out) throws Exception {
                        int count = 0;
                        for(PaidWarn val : values) {
                            if(val != null && !val.isFlag()) {
                                count += 1;
                            }
                        }
                        // ?????????????????????12???
                        int snow = Integer.parseInt(DateUtil.format(window.getEnd(), "HH"));
                        if(snow > 7 && snow < 24 && count == 0) {
                            System.out.println("start:" + DateUtil.format(window.getStart(), "yyyy-MM-dd HH:mm:ss"));
                            System.out.println("count == 0");
                        } else {
                            System.out.println("start:" + DateUtil.format(window.getStart(), "yyyy-MM-dd HH:mm:ss"));
                            System.out.println("count != 0");
                        }
                        out.collect(count);
                    }
                });
    }
}
