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

mvn package
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

```
·bootstrap.servers

在启动consumer时配置的broker地址的。不需要将cluster中所有的broker都配置上，因为启动后会自动的发现cluster所有的broker。

    它配置的格式是：host1:port1;host2:port2…

·key.descrializer、value.descrializer

Message record 的key, value的反序列化类。

·group.id

用于表示该consumer想要加入到哪个group中。默认值是 “”。

·heartbeat.interval.ms

心跳间隔。心跳是在consumer与coordinator之间进行的。心跳是确定consumer存活，加入或者退出group的有效手段。

    这个值必须设置的小于session.timeout.ms，因为：

当Consumer由于某种原因不能发Heartbeat到coordinator时，并且时间超过session.timeout.ms时，就会认为该consumer已退出，它所订阅的partition会分配到同一group 内的其它的consumer上。

    通常设置的值要低于session.timeout.ms的1/3。

    默认值是：3000 （3s）

·session.timeout.ms

Consumer session 过期时间。这个值必须设置在broker configuration中的group.min.session.timeout.ms 与 group.max.session.timeout.ms之间。

其默认值是：10000 （10 s）

 

·enable.auto.commit

Consumer 在commit offset时有两种模式：自动提交，手动提交。手动提交在前面已经说过。自动提交：是Kafka Consumer会在后台周期性的去commit。

默认值是true。

·auto.commit.interval.ms

    自动提交间隔。范围：[0,Integer.MAX]，默认值是 5000 （5 s）

 

·auto.offset.reset

    这个配置项，是告诉Kafka Broker在发现kafka在没有初始offset，或者当前的offset是一个不存在的值（如果一个record被删除，就肯定不存在了）时，该如何处理。它有4种处理方式：

1） earliest：自动重置到最早的offset。

2） latest：看上去重置到最晚的offset。

3） none：如果边更早的offset也没有的话，就抛出异常给consumer，告诉consumer在整个consumer group中都没有发现有这样的offset。

4） 如果不是上述3种，只抛出异常给consumer。

 

默认值是latest。

 

·connections.max.idle.ms

连接空闲超时时间。因为consumer只与broker有连接（coordinator也是一个broker），所以这个配置的是consumer到broker之间的。

默认值是：540000 (9 min)

 

·fetch.max.wait.ms

Fetch请求发给broker后，在broker中可能会被阻塞的（当topic中records的总size小于fetch.min.bytes时），此时这个fetch请求耗时就会比较长。这个配置就是来配置consumer最多等待response多久。

 

·fetch.min.bytes

当consumer向一个broker发起fetch请求时，broker返回的records的大小最小值。如果broker中数据量不够的话会wait，直到数据大小满足这个条件。

取值范围是：[0, Integer.Max]，默认值是1。

默认值设置为1的目的是：使得consumer的请求能够尽快的返回。

 

·fetch.max.bytes

一次fetch请求，从一个broker中取得的records最大大小。如果在从topic中第一个非空的partition取消息时，如果取到的第一个record的大小就超过这个配置时，仍然会读取这个record，也就是说在这片情况下，只会返回这一条record。

    broker、topic都会对producer发给它的message size做限制。所以在配置这值时，可以参考broker的message.max.bytes 和 topic的max.message.bytes的配置。

取值范围是：[0, Integer.Max]，默认值是：52428800 （5 MB）

 

 

·max.partition.fetch.bytes

一次fetch请求，从一个partition中取得的records最大大小。如果在从topic中第一个非空的partition取消息时，如果取到的第一个record的大小就超过这个配置时，仍然会读取这个record，也就是说在这片情况下，只会返回这一条record。

    broker、topic都会对producer发给它的message size做限制。所以在配置这值时，可以参考broker的message.max.bytes 和 topic的max.message.bytes的配置。

 

·max.poll.interval.ms

前面说过要求程序中不间断的调用poll()。如果长时间没有调用poll，且间隔超过这个值时，就会认为这个consumer失败了。

 

·max.poll.records

    Consumer每次调用poll()时取到的records的最大数。

 

·receive.buffer.byte

Consumer receiver buffer （SO_RCVBUF）的大小。这个值在创建Socket连接时会用到。

取值范围是：[-1, Integer.MAX]。默认值是：65536 （64 KB）

如果值设置为-1，则会使用操作系统默认的值。

 

·request.timeout.ms

请求发起后，并不一定会很快接收到响应信息。这个配置就是来配置请求超时时间的。默认值是：305000 （305 s）

 

·client.id

Consumer进程的标识。如果设置一个人为可读的值，跟踪问题会比较方便。

 

·interceptor.classes

    用户自定义interceptor。

·metadata.max.age.ms

Metadata数据的刷新间隔。即便没有任何的partition订阅关系变更也行执行。

范围是：[0, Integer.MAX]，默认值是：300000 （5 min）
```