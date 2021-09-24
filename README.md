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

```
kafka.brokers=localhost:9092
kafka.group.id=peter-metrics-group-test
kafka.zookeeper.connect=localhost:2181
metrics.topic=peter-metrics
stream.parallelism=5
stream.checkpoint.interval=1000
stream.checkpoint.enable=false
elasticsearch.hosts=localhost:9200
elasticsearch.bulk.flush.max.actions=40
stream.sink.parallelism=5

1、bulk.flush.backoff.enable 用来表示是否开启重试机制
2、bulk.flush.backoff.type 重试策略，有两种：EXPONENTIAL 指数型（表示多次重试之间的时间间隔按照指数方式进行增长）、CONSTANT 常数型（表示多次重试之间的时间间隔为固定常数）
3、bulk.flush.backoff.delay 进行重试的时间间隔
4、bulk.flush.backoff.retries 失败重试的次数
5、bulk.flush.max.actions: 批量写入时的最大写入条数
6、bulk.flush.max.size.mb: 批量写入时的最大数据量
7、bulk.flush.interval.ms: 批量写入的时间间隔，配置后则会按照该时间间隔严格执行，无视上面的两个批量写入配置

DataStream<String> input = ...;

input.addSink(new ElasticsearchSink<>(
    config, transportAddresses,
    new ElasticsearchSinkFunction<String>() {...},
    new ActionRequestFailureHandler() {
        @Override
        void onFailure(ActionRequest action,
                Throwable failure,
                int restStatusCode,
                RequestIndexer indexer) throw Throwable {

            if (ExceptionUtils.containsThrowable(failure, EsRejectedExecutionException.class)) {
                // full queue; re-add document for indexing
                indexer.add(action);
            } else if (ExceptionUtils.containsThrowable(failure, ElasticsearchParseException.class)) {
                // malformed document; simply drop request without failing sink
            } else {
                // for all other failures, fail the sink
                // here the failure is simply rethrown, but users can also choose to throw custom exceptions
                throw failure;
            }
        }
}));

bin/kafka-topics.sh --list --zookeeper localhost:2181
bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic metric-test


docker run -d  -p 15672:15672  -p  5672:5672  -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin --name rabbitmq rabbitmq:3-management

web.tmpdir: /usr/local/blink-1.5.1/jars
web.upload.dir: /usr/local/blink-1.5.1/jars
```

```
mvn install:install-file "-DgroupId=org.apache" "-DartifactId=doris-flink" "-Dversion=1.0-SNAPSHOT" "-Dpackaging=jar" "-Dfile=doris-flink-1.0-SNAPSHOT.jar"

flink run -c com.github.zhangchunsheng.flink.SocketTextStreamWordCount ~/dev/github/flink-demo/target/original-flink-demo-1.0-SNAPSHOT.jar 127.0.0.1 9000
~/git/flink-1.10.0/bin/flink run -c com.github.zhangchunsheng.flink.TestDorisSourceJob target/flink-demo-1.0-SNAPSHOT.jar

flink-1.11.2
~/git/flink-1.11.2/bin/flink run -c com.github.zhangchunsheng.flink.TestDorisSourceJob target/flink-demo-1.0-SNAPSHOT.jar

~/git/flink-1.10.0/bin/flink run -c com.github.zhangchunsheng.flink.pomegranate.EquipmentStatusSinkDoris target/flink-demo-1.0-SNAPSHOT.jar
~/git/flink-1.10.0/bin/flink run -c com.github.zhangchunsheng.flink.pomegranate.EquipmentStatusSinkKafka target/flink-demo-1.0-SNAPSHOT.jar

bin/kafka-consumer-groups.sh --bootstrap-server 192.168.0.200:9092 --describe --group equipment-status-group-1
bin/kafka-consumer-groups.sh --bootstrap-server 192.168.0.200:9092 --describe --group flink_doris_camtg_group_equipment_status1

mvn package
flink 窗口
```

```
https://cloud.tencent.com/developer/article/1647308
https://cloud.tencent.com/developer/article/1764048

https://cloud.tencent.com/developer/article/1633494
```

```
kafka 消费慢
flink kafka 消费慢

FlinkKafkaConsumer011
```
