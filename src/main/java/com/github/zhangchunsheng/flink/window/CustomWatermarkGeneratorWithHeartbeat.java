package com.github.zhangchunsheng.flink.window;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class CustomWatermarkGeneratorWithHeartbeat implements WatermarkGenerator<Message> {
    /**
     * 最大允许乱序时间  单位ms
     */
    private static final Long MAX_OUT_OF_ORDER_TIME = 3000L;

    /**
     * 当前watermark时间戳
     */
    private long currentMaxTimestamp = -1L;

    private static final String HEARTBEAT_NAME = "heartbeat";

    /**
     * 心跳等待次数
     * <p>
     * 心跳等待时间 = 心跳等待次数 * 心跳间隔时间
     * <p>
     * 心跳等待时间 >= MAX_OUT_OF_ORDER_TIME + WINDOW_SIZE 才能触发窗口内剩余数据计算
     */
    private static final int MAX_HEARTBEAT_TIMES = 6;

    /**
     * 心跳间隔时间
     */
    private static final int MAX_HEARTBEAT_INTERVAL = 1000;

    /**
     * watermark未更新后的心跳计数
     */
    int heartbeatTimes = 0;

    /**
     * 数据流中断状态
     */
    boolean isStreamingBreak = true;

    @Override
    public void onEvent(Message message, long l, WatermarkOutput watermarkOutput) {
        if (!message.value.equals(HEARTBEAT_NAME)) {
            // 正常数据源，根据时间戳推动watermark
            // 数据时间超过了currentMaxTimestamp，则更新watermark
            currentMaxTimestamp = Math.max(currentMaxTimestamp, message.timestamp);
            // 初始化心跳状态
            heartbeatTimes = 0;
            isStreamingBreak = false;
        } else { // 心跳数据源处理方法
            // 心跳数据记录+1
            heartbeatTimes++;
            if (heartbeatTimes >= MAX_HEARTBEAT_TIMES && !isStreamingBreak) {
                // 当连续心跳计数超过最大心跳等待次数时，证明正常数据出现断点，此时watermark向前推进
                // 推进时间为 心跳等待时间
                currentMaxTimestamp += MAX_HEARTBEAT_TIMES * MAX_HEARTBEAT_INTERVAL;
                // 更新数据流状态，避免后续的心跳数据再次推动watermark
                isStreamingBreak = true;
            }
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - MAX_OUT_OF_ORDER_TIME - 1));
    }
}
