package com.github.zhangchunsheng.flink.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TumblingEventWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Message> sourceStream =
                env.addSource(new SourceFunction<Message>() {
                    // 每次发送数据的时间间隔为1s
                    private static final long INTERVAL = 1000;

                    @Override
                    public void run(SourceContext<Message> sourceContext) throws Exception {
                        long timestamp = System.currentTimeMillis();
                        for (int i = 1; i < Integer.MAX_VALUE; i++, timestamp += INTERVAL) { // 每秒发送1条消息
                            sourceContext.collect(new Message(timestamp, "value=" + i));
                            Thread.sleep(INTERVAL);
                            if (i % 10 == 0) { // 每发送10条数据，等待20s
                                Thread.sleep(20 * 1000);
                                // 每次等待时，事件时间向前推进一个窗口，确保前后两段数据没有关联
                                timestamp += 2 * 1000;
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                    }
                });

        // 心跳数据源
        DataStreamSource<Message> heartbeatSourceStream =
                env.addSource(new SourceFunction<Message>() {
                    @Override
                    public void run(SourceContext<Message> sourceContext) throws Exception {
                        for (int i = 1; i < Integer.MAX_VALUE; i++) { // 每秒发送1条心跳消息
                            sourceContext.collect(new Message(System.currentTimeMillis(), "heartbeat"));
                            Thread.sleep(1000);
                        }
                    }

                    @Override
                    public void cancel() {
                    }
                });

        // 标记watermark
        DataStream<Message> sourceStreamWithWatermark =
                sourceStream
                        .union(heartbeatSourceStream)
                        .assignTimestampsAndWatermarks((
                                // 使用自定义WatermarkGenerator
                                (WatermarkStrategy<Message>) context -> new CustomWatermarkGenerator())
                                // 标记时间戳字段（kafka等数据源可自动识别，但是自定义类需手动标记时间戳字段）
                                .withTimestampAssigner((message, recordTimestamp) -> message.timestamp)
                                // 配置5分钟没有数据输入，则标记为idle
                                .withIdleness(Duration.ofSeconds(5))
                        )
                        .filter((FilterFunction<Message>) value -> !"heartbeat".equals(value.value));

        // 构建窗口，处理数据
        DataStream<List<Message>> transformStream =
                sourceStreamWithWatermark
                        .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
                        .process(new ProcessAllWindowFunction<Message, List<Message>, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<Message> iterable, Collector<List<Message>> collector) {
                                List<Message> messages = new ArrayList<>();
                                iterable.forEach(messages::add);
                                collector.collect(messages);
                            }
                        });

        // sink打印每个窗口的结果
        transformStream.addSink(new SinkFunction<List<Message>>() {
            @Override
            public void invoke(List<Message> value, Context context) {
                String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                System.out.println(now + "\t\t" + value);
            }
        });
        transformStream.print();

        env.execute("TumblingEventWindowExample");
    }
}
