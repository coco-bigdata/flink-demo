package com.github.zhangchunsheng.flink.window;

import java.time.Instant;
import java.util.Properties;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

public class FlinkStreamTimeoutDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.200:9092,192.168.0.160:9092,192.168.0.178:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>("t-test", new SimpleStringSchema(),
                properties);

        DataStream<String> stream = env.addSource(myConsumer);
        // 业务数据流

        DataStream<String> out = stream.keyBy(new DeviceIdSelector())
                .process(new TimeoutFunction(5 * 1000)).map(new SimpleMapFunction());

        out.print(); // 输出

        env.execute();
        System.out.println("OK");
    }

    private static class SimpleMapFunction implements MapFunction<Alert, String> {
        /**
         *
         */
        private static final long serialVersionUID = 9138593491263188285L;
        private ObjectMapper mapper;

        public SimpleMapFunction() {
            mapper = new ObjectMapper();
        }

        @Override
        public String map(Alert value) throws Exception {
            return mapper.writeValueAsString(value);
        }

    }

    private static class DeviceIdSelector implements KeySelector<String, String> {
        /**
         *
         */
        private static final long serialVersionUID = -1786501262400329204L;
        private ObjectMapper mapper;

        public DeviceIdSelector() {
            mapper = new ObjectMapper();
        }

        @Override
        public String getKey(String value) throws Exception {
            JsonNode node = mapper.readValue(value, JsonNode.class);
            return node.get("deviceId").asText();
        }

    }

    private static class TimeoutFunction extends KeyedProcessFunction<String, String, Alert> {
        /**
         *
         */
        private static final long serialVersionUID = -189375175964554333L;
        private final long defaultTimeout;
        private transient ValueState<Long> lastTimer;
        private transient ValueState<Integer> timeoutFactor;

        public TimeoutFunction(long timeout) {
            this.defaultTimeout = timeout;
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, String, Alert>.OnTimerContext ctx,
                            Collector<Alert> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 获取Key， deviceId是keyBy键
            String deviceId = ctx.getCurrentKey();
            // 超时输出告警
            if (timestamp == lastTimer.value()) {
                // 增加告警延迟，如果没有持续数据，递增
                Integer factor = timeoutFactor.value() + 1;
                long timeout = factor * defaultTimeout;
                updateTimer(ctx, timeout);
                timeoutFactor.update(factor);
                out.collect(new Alert(deviceId, "HIGH", "Data Stream Timeout"));
            }
        }

        @Override
        public void processElement(String value, KeyedProcessFunction<String, String, Alert>.Context ctx,
                                   Collector<Alert> out) throws Exception {
            // 更新定时器
            updateTimer(ctx, defaultTimeout);
            // 有数据进入，还原因子
            timeoutFactor.update(1);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 注册状态
            ValueStateDescriptor<Long> lastTimerDescriptor = new ValueStateDescriptor<>("lastTimer", Long.class);
            lastTimer = getRuntimeContext().getState(lastTimerDescriptor);

            ValueStateDescriptor<Integer> timeoutFactorDescriptor = new ValueStateDescriptor<>("timeoutFactor",
                    Integer.class);
            timeoutFactor = getRuntimeContext().getState(timeoutFactorDescriptor);
        }

        private void updateTimer(KeyedProcessFunction<String, String, Alert>.Context ctx, long timeout)
                throws Exception {
            // 获取当前时间
            long current = ctx.timerService().currentProcessingTime();
            // 延长超时时间
            long nextTimeout = current + timeout;
            // 注册定时器
            ctx.timerService().registerProcessingTimeTimer(nextTimeout);
            // 更新状态
            lastTimer.update(nextTimeout);
        }
    }
}
