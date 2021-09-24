package com.github.zhangchunsheng.flink.pomegranate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PaidWarningProcessFunction extends KeyedProcessFunction<Integer, PaidWarn, PaidWarn> {
    private ValueState<PaidWarn> paidWarnState;
    private int delay;

    PaidWarningProcessFunction(int delay) {
        this.delay = delay;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<PaidWarn> paidInfo = TypeInformation.of(new TypeHint<PaidWarn>() {
        });
        paidWarnState = getRuntimeContext().getState(new ValueStateDescriptor<PaidWarn>("paid", paidInfo));
    }

    @Override
    public void processElement(PaidWarn paidWarn, Context context, Collector<PaidWarn> collector) throws Exception {
        PaidWarn current = paidWarnState.value();
        if(current == null) {
            current = new PaidWarn(true, paidWarn.getKey(), paidWarn.getPaidCreateTime(), paidWarn.getNumOrTimestamp());
        } else {
            if(paidWarn.getKey() != null) {
                current.setKey(paidWarn.getKey());
            }
            if(paidWarn.getNumOrTimestamp() != null) {
                current.setNumOrTimestamp(paidWarn.getNumOrTimestamp());
            }
            if(paidWarn.getOrderId() != null) {
                current.setOrderId(paidWarn.getOrderId());
            }
            current.setSystemTimestamp(System.currentTimeMillis());
        }
        current.setPaidCreateTime(paidWarn.getPaidCreateTime());
        paidWarnState.update(current);
        context.timerService().registerProcessingTimeTimer(current.getSystemTimestamp() + delay);
    }
}
