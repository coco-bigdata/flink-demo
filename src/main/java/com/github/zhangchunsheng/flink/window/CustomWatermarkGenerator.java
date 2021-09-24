package com.github.zhangchunsheng.flink.window;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class CustomWatermarkGenerator implements WatermarkGenerator<Message> {
    /**
     * 最大允许乱序时间  单位ms
     */
    private static final Long MAX_OUT_OF_ORDER_TIME = 3000L;

    /**
     * 当前watermark时间戳
     */
    private long currentMaxTimestamp = -1L;

    @Override
    public void onEvent(Message message, long l, WatermarkOutput watermarkOutput) {
        // 数据时间超过了currentMaxTimestamp，则更新watermark
        currentMaxTimestamp = Math.max(currentMaxTimestamp, message.timestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - MAX_OUT_OF_ORDER_TIME - 1));
    }
}
