package com.github.zhangchunsheng.flink;

import com.github.zhangchunsheng.flink.model.MetricEvent;
import com.github.zhangchunsheng.flink.schemas.MetricSchema;
import com.github.zhangchunsheng.flink.utils.ExecutionEnvUtil;
import com.github.zhangchunsheng.flink.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class TestKafkaSink {
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
}
