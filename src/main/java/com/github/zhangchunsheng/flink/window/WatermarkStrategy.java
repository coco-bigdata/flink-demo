package com.github.zhangchunsheng.flink.window;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;

import java.time.Duration;

public interface WatermarkStrategy<T>
        extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T> {
    TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context);
    WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);
    WatermarkStrategy<T> withIdleness(Duration idleTimeout);
}
