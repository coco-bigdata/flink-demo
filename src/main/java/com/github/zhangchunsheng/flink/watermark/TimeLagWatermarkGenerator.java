package com.github.zhangchunsheng.flink.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<MyEvent> {
    private final long maxTimeLag = 3000; // 3 seconds

    @Override
    public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
        return element.getCreationTime();
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current time minus the maximum time lag
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }
}
