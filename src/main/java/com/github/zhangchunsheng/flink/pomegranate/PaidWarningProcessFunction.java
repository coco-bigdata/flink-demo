package com.github.zhangchunsheng.flink.pomegranate;

import com.github.zhangchunsheng.flink.utils.DateUtil;
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

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PaidWarn> out) throws Exception {
        System.out.println("onTimer timestamp:" + DateUtil.format(timestamp, "yyyy-MM-dd HH:mm:ss"));
        PaidWarn result = paidWarnState.value();
        if(result != null && timestamp >= result.getSystemTimestamp() + delay && System.currentTimeMillis()- result.getPaidCreateTime() > 0) {
            int snow = Integer.parseInt(DateUtil.format(System.currentTimeMillis(), "HH"));
            if(snow > 7 && snow < 24) {
                System.out.println("send");
            }
            out.collect(result);
            ctx.timerService().registerProcessingTimeTimer(timestamp + delay);
            result.setSystemTimestamp(timestamp);
            paidWarnState.update(result);
        } else {
            System.out.println("no send");
        }
    }
}
