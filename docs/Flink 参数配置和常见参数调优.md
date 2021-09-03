Flink参数配置
    jobmanger.rpc.address jm的地址。
    
    jobmanager.rpc.port jm的端口号。
    
    jobmanager.heap.mb jm的堆内存大小。不建议配的太大，1-2G足够。
    
    taskmanager.heap.mb tm的堆内存大小。大小视任务量而定。需要存储任务的中间值，网络缓存，用户数据等。
    
    taskmanager.numberOfTaskSlots slot数量。在yarn模式使用的时候会受到yarn.scheduler.maximum-allocation-vcores值的影响。此处指定的slot数量如果超过yarn的maximum-allocation-vcores，flink启动会报错。在yarn模式，flink启动的task manager个数可以参照如下计算公式：
    
    num_of_tm = ceil(parallelism / slot)  即并行度除以slot个数，结果向上取整。
    
    parallelsm.default 任务默认并行度，如果任务未指定并行度，将采用此设置。
    
    web.port Flink web ui的端口号。
    
    jobmanager.archive.fs.dir 将已完成的任务归档存储的目录。
    
    history.web.port 基于web的history server的端口号。
    
    historyserver.archive.fs.dir history server的归档目录。该配置必须包含jobmanager.archive.fs.dir配置的目录，以便history server能够读取到已完成的任务信息。
    
    historyserver.archive.fs.refresh-interval 刷新存档作业目录时间间隔
    
    state.backend 存储和检查点的后台存储。可选值为rocksdb filesystem hdfs。
    
    state.backend.fs.checkpointdir 检查点数据文件和元数据的默认目录。
    
    state.checkpoints.dir 保存检查点目录。
    
    state.savepoints.dir save point的目录。
    
    state.checkpoints.num-retained 保留最近检查点的数量。
    
    state.backend.incremental 增量存储。
    
    akka.ask.timeout Job Manager和Task Manager通信连接的超时时间。如果网络拥挤经常出现超时错误，可以增大该配置值。
    
    akka.watch.heartbeat.interval 心跳发送间隔，用来检测task manager的状态。
    
    akka.watch.heartbeat.pause 如果超过该时间仍未收到task manager的心跳，该task manager 会被认为已挂掉。
    
    taskmanager.network.memory.max 网络缓冲区最大内存大小。
    
    taskmanager.network.memory.min 网络缓冲区最小内存大小。
    
    taskmanager.network.memory.fraction 网络缓冲区使用的内存占据总JVM内存的比例。如果配置了taskmanager.network.memory.max和taskmanager.network.memory.min，本配置项会被覆盖。
    
    fs.hdfs.hadoopconf hadoop配置文件路径（已被废弃，建议使用HADOOP_CONF_DIR环境变量）
    
    yarn.application-attempts job失败尝试次数，主要是指job manager的重启尝试次数。该值不应该超过yarn-site.xml中的yarn.resourcemanager.am.max-attemps的值。

Flink HA(Job Manager)的配置
    high-availability: zookeeper 使用zookeeper负责HA实现
    
    high-availability.zookeeper.path.root: /flink flink信息在zookeeper存储节点的名称
    
    high-availability.zookeeper.quorum: zk1,zk2,zk3 zookeeper集群节点的地址和端口
    
    high-availability.storageDir: hdfs://nameservice/flink/ha/ job manager元数据在文件系统储存的位置，zookeeper仅保存了指向该目录的指针。

Flink metrics 监控相关配置
    metrics.reporters: prom
    
    metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
    
    metrics.reporter.prom.port: 9250-9260

Kafka相关调优配置
    linger.ms/batch.size 这两个配置项配合使用，可以在吞吐量和延迟中得到最佳的平衡点。batch.size是kafka producer发送数据的批量大小，当数据量达到batch size的时候，会将这批数据发送出去，避免了数据一条一条的发送，频繁建立和断开网络连接。但是如果数据量比较小，导致迟迟不能达到batch.size，为了保证延迟不会过大，kafka不能无限等待数据量达到batch.size的时候才发送。为了解决这个问题，引入了linger.ms配置项。当数据在缓存中的时间超过linger.ms时，无论缓存中数据是否达到批量大小，都会被强制发送出去。
    
    ack 数据源是否需要kafka得到确认。all表示需要收到所有ISR节点的确认信息，1表示只需要收到kafka leader的确认信息，0表示不需要任何确认信息。该配置项需要对数据精准性和延迟吞吐量做出权衡。

Kafka topic分区数和Flink并行度的关系
    Flink kafka source的并行度需要和kafka topic的分区数一致。最大化利用kafka多分区topic的并行读取能力。

Yarn相关调优配置
    yarn.scheduler.maximum-allocation-vcores
    
    yarn.scheduler.minimum-allocation-vcores
    
    Flink单个task manager的slot数量必须介于这两个值之间
    
    yarn.scheduler.maximum-allocation-mb
    
    yarn.scheduler.minimum-allocation-mb
    
    Flink的job manager 和task manager内存不得超过container最大分配内存大小。
    
    yarn.nodemanager.resource.cpu-vcores yarn的虚拟CPU内核数，建议设置为物理CPU核心数的2-3倍，如果设置过少，会导致CPU资源无法被充分利用，跑任务的时候CPU占用率不高。