# Milvus-like 向量数据库系统

基于Java实现的类Milvus架构的分布式向量数据库系统，包含TSO分发、Kafka日志消费、负载均衡等核心功能。

## 系统架构

```
milvus-like/
├── common/                  // 公共模块
│   ├── TSO.java             // TSO 实体（时间戳 + 计数器）
│   ├── GrpcClient.java      // gRPC 通信工具类
│   ├── EtcdClient.java      // Etcd 客户端（元数据操作）
│   └── KafkaConsumerConfig.java // Kafka 消费配置
├── coordinator/             // 协调服务层
│   ├── root/
│   │   ├── RootCoordinatorServer.java // TSO 生成、元数据管理
│   │   └── TSOGenerator.java          // TSO 生成逻辑
│   └── data/
│       ├── DataCoordinatorServer.java // Segment 分配、Flush 触发
│       └── DataNodeLoadBalancer.java  // DataNode 负载均衡算法
├── node/                    // 工作节点层
│   └── data/
│       ├── DataNodeServer.java      // 消费 Kafka 日志，维护缓冲区
│       ├── SegmentFlushManager.java // 缓冲区 Flush 管理
│       └── SegmentBuffer.java       // 数据缓冲区实现
└── storage/                 // 存储层客户端
    ├── KafkaManager.java    // Kafka 生产/消费封装
    └── EtcdManager.java     // 元数据 CRUD 封装
```

## 核心技术特性

### 1. TSO (Timestamp Oracle) 分发
- **全局唯一时间戳**: 42位物理时间戳 + 22位逻辑计数器
- **高性能生成**: 支持批量TSO分配，减少锁竞争
- **单调递增**: 保证分布式环境下的时序一致性

### 2. Kafka 日志消费
- **多类型日志**: 支持插入日志和删除日志分离消费
- **负载均衡**: 不同节点类型使用不同的消费配置
- **容错机制**: 手动提交offset，支持消费重试

### 3. 负载均衡算法
- **轮询 (Round Robin)**: 简单均匀分配
- **最少负载 (Least Load)**: 基于节点当前负载选择
- **一致性哈希 (Consistent Hash)**: 保证数据分布稳定性

### 4. 数据缓冲与刷盘
- **内存缓冲**: 高效的并发数据缓冲区
- **智能Flush**: 基于大小、记录数、时间的多重触发条件
- **异步刷盘**: 非阻塞的数据持久化

## 快速开始

### 环境要求
- Java 11+
- Apache Kafka 2.8+
- Etcd 3.5+
- Maven 3.6+

### 启动依赖服务

1. **启动Etcd**
```bash
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379
```

2. **启动Kafka**
```bash
# 启动Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# 启动Kafka
bin/kafka-server-start.sh config/server.properties

# 创建主题
bin/kafka-topics.sh --create --topic milvus-insert-log --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic milvus-delete-log --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 编译运行

```bash
# 编译项目
mvn clean compile

# 运行系统
mvn exec:java -Dexec.mainClass="com.milvus.MilvusApplication"
```

## 核心组件说明

### Root Coordinator
- **职责**: TSO生成、Leader选举、服务注册发现
- **端口**: 19530
- **关键功能**:
  - 全局TSO分配
  - 集群元数据管理
  - 节点健康监控

### Data Coordinator  
- **职责**: Segment分配、Flush调度、数据节点管理
- **端口**: 13333
- **关键功能**:
  - 智能负载均衡
  - 自动Flush触发
  - 节点故障检测

### Data Node
- **职责**: Kafka日志消费、数据缓冲、持久化存储
- **端口**: 21121
- **关键功能**:
  - 高效数据缓冲
  - 异步Flush机制
  - 负载状态报告

## 配置参数

### TSO配置
```java
// 逻辑计数器最大值 (22位)
private static final long MAX_LOGICAL = 0x3FFFFF;

// 批量分配大小
private static final int BATCH_SIZE = 1000;
```

### 缓冲区配置
```java
// 最大缓冲区大小
private final long maxBufferSize = 256 * 1024 * 1024; // 256MB

// 最大记录数
private final int maxBufferRecords = 100000;

// Flush间隔
private final long flushInterval = 30000; // 30秒
```

### Kafka配置
```java
// 消费者组配置
props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
```

## 监控与运维

### 日志级别
- **INFO**: 系统启动、关闭、重要状态变更
- **DEBUG**: 详细的处理流程、性能指标
- **TRACE**: 最细粒度的调试信息

### 健康检查
- 节点心跳检测 (10秒间隔)
- 内存使用监控 (90%阈值)
- Kafka连接状态检查

### 性能指标
- TSO生成速率
- Kafka消费延迟
- Flush操作耗时
- 节点负载分布

## 扩展开发

### 添加新的负载均衡算法
```java
public enum Strategy {
    ROUND_ROBIN,
    LEAST_LOAD,
    CONSISTENT_HASH,
    CUSTOM_ALGORITHM  // 新增算法
}
```

### 自定义Flush策略
```java
public boolean shouldFlush() {
    // 实现自定义Flush触发逻辑
    return customFlushCondition();
}
```

### 集成对象存储
```java
// 实现ObjectStorageClient接口
public interface ObjectStorageClient {
    String uploadSegment(String segmentId, byte[] data);
    byte[] downloadSegment(String location);
}
```

## 注意事项

1. **生产环境部署**: 需要配置适当的JVM参数和资源限制
2. **数据一致性**: TSO机制保证了分布式事务的一致性
3. **故障恢复**: 支持节点故障自动检测和负载重分配
4. **性能调优**: 可根据实际负载调整缓冲区大小和Flush策略

## 许可证

MIT License