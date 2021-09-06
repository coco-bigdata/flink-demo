package com.github.zhangchunsheng.flink;

import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class TestDorisSourceJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("fenodes","192.168.0.186:8030");
        properties.put("username","root");
        properties.put("password","camtg");
        properties.put("table.identifier","camtg.c_original_data_t");

        env.addSource(new DorisSourceFunction(new DorisStreamOptions(properties), new SimpleListDeserializationSchema())).print();

        env.execute("Flink add doris source");
    }
}
