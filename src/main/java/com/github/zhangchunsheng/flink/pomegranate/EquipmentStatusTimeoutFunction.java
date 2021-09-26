package com.github.zhangchunsheng.flink.pomegranate;

import com.github.zhangchunsheng.flink.utils.DateUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class EquipmentStatusTimeoutFunction extends KeyedProcessFunction<String, Tuple2<String, Map<String,  String>>, Tuple2<String, Map<String, String>>>  {
    /**
     *
     */
    private static final long serialVersionUID = -189375175964554333L;
    private final long defaultTimeout;
    private transient ValueState<Long> lastTimer;
    private transient ValueState<Integer> timeoutFactor;

    public EquipmentStatusTimeoutFunction(long timeout) {
        this.defaultTimeout = timeout;
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Map<String,  String>>, Tuple2<String, Map<String, String>>>.OnTimerContext ctx,
                        Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        // 获取Key， deviceId是keyBy键
        String equipmentNumber = ctx.getCurrentKey();
        // 超时输出告警
        if (timestamp == lastTimer.value()) {
            // 增加告警延迟，如果没有持续数据，递增
            Integer factor = timeoutFactor.value() + 1;
            long timeout = factor * defaultTimeout;
            updateTimer(ctx, timeout);
            timeoutFactor.update(factor);
            // make data status=-2 status 5s后又上传
            Tuple2<String, Map<String, String>> result = new Tuple2<>();
            result.f0 = equipmentNumber;
            Map<String, String> map = new HashMap();
            Long packageTime = ctx.timerService().currentProcessingTime();
            map.put("package_time", String.valueOf(packageTime));
            map.put("package_date", DateUtil.getDay(packageTime));
            map.put("status", "-2");
            map.put("equipment_number", equipmentNumber);
            map.put("ip", "-1.-1.-1.-1");
            map.put("package_no", "-1");
            map.put("work_time", "-1");
            map.put("standby_time", "-1");
            map.put("warning_time", "-1");
            map.put("piece_cnt", "-1");
            result.f1 = map;
            out.collect(result);
        }
    }

    @Override
    public void processElement(Tuple2<String, Map<String, String>> value, KeyedProcessFunction<String, Tuple2<String, Map<String,  String>>, Tuple2<String, Map<String, String>>>.Context ctx,
                               Collector<Tuple2<String, Map<String, String>>> out) throws Exception {
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

    private void updateTimer(KeyedProcessFunction<String, Tuple2<String, Map<String,  String>>, Tuple2<String, Map<String, String>>>.Context ctx, long timeout)
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
