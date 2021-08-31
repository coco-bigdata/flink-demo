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
Window / WindowAll / Union / Window join / Split / Select / Project 等，操作很多，可以将数据转换计算成你想要的数据
```

Sink

```
Kafka、ElasticSearch、Socket、RabbitMQ、JDBC、Cassandra POJO、File、Print
```