package com.github.zhangchunsheng.flink.window;

import javafx.scene.control.Alert;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.awt.*;

class TimeoutFunction extends KeyedProcessFunction<Tuple, Event, Event> {
    private final long timeout;
    private transient ValueState<Long> lastTimer;
    private final OutputTag<Alert> sideOutput;

    public TimeoutFunction(long timeout, OutputTag<Alert> sideOutput) {
        this.timeout = timeout;
        this.sideOutput = sideOutput;
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        ValueStateDescriptor<Long> lastTimerDesc = new ValueStateDescriptor<>("lastTimer", Long.class);
        lastTimer = this.getRuntimeContext().getState(lastTimerDesc);
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> collector) throws Exception {
        long current = ctx.timerService().currentProcessingTime();
        long nextTimeout = current + timeout;
        ctx.timerService().registerProcessingTimeTimer(nextTimeout); // 注册Timer
        lastTimer.update(nextTimeout); // 记录超时时间
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Event> out) throws Exception {
        String id = ctx.getCurrentKey().getField(0);
        if (ts == lastTimer.value()) { // timer的时间是否超时时间
            ctx.output(sideOutput, new Alert(Alert.AlertType.WARNING, id));
        }
    }
}
