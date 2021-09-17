package com.github.zhangchunsheng.flink.status

import lombok.AllArgsConstructor

import lombok.ToString

import org.apache.flink.api.common.state.StateTtlConfig

import org.apache.flink.api.common.state.ValueStateDescriptor

import lombok.`val`

import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.functions.KeySelector

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


class TestStatusJob {
    @kotlin.Throws(Exception::class)
    @kotlin.jvm.JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.streamTimeCharacteristic = TimeCharacteristic.EventTime
        env.parallelism = 1
        val dataStream = env.addSource(ReadLineSource("src/main/resources/data.txt"))
        val process: Any = dataStream
            .flatMap(object : FlatMapFunction<String?, Tuple2<String?, Long?>?> {
                @kotlin.Throws(Exception::class)
                fun flatMap(s: String, out: Collector<Tuple2<String?, Long?>?>) {
                    val split: Array<String> = s.split(",").toTypedArray()
                    if ("pv" == split[3]) {
                        val res = Tuple2(split[0] + "-" + split[1], split[4].toLong())
                        out.collect(res)
                    }
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .< Tuple2 < String,
                Long > > forBoundedOutOfOrderness < Tuple2 < kotlin . String ?,
                Long? > ? > Duration.ofMillis(1000)
            .withTimestampAssigner(SerializableTimestampAssigner<Tuple2<String, Long>> { s, l -> s.f1 } as SerializableTimestampAssigner<Tuple2<String?, Long?>?>?))
        .keyBy<Any>(KeySelector<R, Any> { s: R -> s.f0 })
            .process(object : KeyedProcessFunction<String?, Tuple2<String?, Long?>?, Any?>() {
                private var state: ValueState<UserBehavior>? = null
                override fun open(parameters: Configuration?) {
                    val stateDescriptor: ValueStateDescriptor<UserBehavior> = ValueStateDescriptor<UserBehavior>(
                        "mystate",
                        UserBehavior::class.java
                    )
                    stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(60)).build())
                    state = runtimeContext.getState(stateDescriptor)
                }

                @kotlin.Throws(Exception::class)
                override fun processElement(
                    `in`: Tuple2<String?, Long?>,
                    ctx: Context,
                    out: Collector<Any?>
                ) {
                    var cur: UserBehavior = state.value()
                    if (cur == null) {
                        cur = UserBehavior(`in`.f0, `in`.f1)
                        state.update(cur)
                        ctx.timerService().registerEventTimeTimer(cur.getTimestamp() + 60000)
                        out.collect(cur)
                    } else {
                        System.out.println("[Duplicate Data] " + `in`.f0.toString() + " " + `in`.f1)
                    }
                }

                @kotlin.Throws(Exception::class)
                override fun onTimer(
                    timestamp: Long,
                    ctx: OnTimerContext?,
                    out: Collector<Any?>?
                ) {
                    val cur: UserBehavior = state.value()
                    if (cur.getTimestamp() + 1000 <= timestamp) {
                        System.out.printf(
                            "[Overdue] now: %d obj_time: %d Date: %s%n",
                            timestamp, cur.getTimestamp(), cur.getId()
                        )
                        state.clear()
                    }
                }
            })
        process.print()
        env.execute("flink")
    }

    @Data
    @ToString
    @AllArgsConstructor
    private class UserBehavior {
        private val id: String? = null
        private val timestamp: Long = 0
    }
}