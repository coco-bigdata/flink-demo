# flink-demo
flink-demo

Source

```
数据源，Flink 在流处理和批处理上的 source 大概有 4 类：
基于本地集合的 source
基于文件的 source
基于网络套接字的 source
自定义的 source
自定义的 source 常见的有 Apache kafka、Amazon Kinesis Streams、
RabbitMQ、Twitter Streaming API、Apache NiFi 等，当然你也可以定义自己的 source
```

Transformation

```
数据转换的各种操作，有 Map / FlatMap / Filter / KeyBy / Reduce / Fold / Aggregations /
Window / WindowAll / Union / Window join / Split / Select / Project 等
可以将数据转换计算成你想要的数据
```

Sink

```
Kafka、ElasticSearch、Socket、RabbitMQ、JDBC、Cassandra POJO、File、Print
接收器，Flink 将转换计算后的数据发送的地点 ，你可能需要存储下来
Flink 常见的 Sink 大概有如下几类：
写入文件
打印出来
写入 socket
自定义的 sink
自定义的 sink 常见的有 Apache kafka、RabbitMQ、MySQL、ElasticSearch
Apache Cassandra、Hadoop FileSystem 等，同理你也可以定义自己的 Sink
```

Time Windows

```
Apache Flink 具有三个不同的时间概念，即 processing time, event time 和 ingestion time
Processing Time 是指事件被处理时机器的系统时间
Event Time 是事件发生的时间，一般就是数据本身携带的时间
Ingestion Time 是事件进入 Flink 的时间
```

```
./bin/flink run -p 10 ../word-count.jar

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(10);

data.keyBy(new xxxKey())
    .flatMap(new XxxFlatMapFunction()).setParallelism(5)
    .map(new XxxMapFunction).setParallelism(5)
    .addSink(new XxxSink()).setParallelism(1)

// 算子设置并行度 > env 设置并行度 > 配置文件默认并行度
```