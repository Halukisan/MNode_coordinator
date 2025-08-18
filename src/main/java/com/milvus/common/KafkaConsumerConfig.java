package com.milvus.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * Kafka 消费者配置
 * 提供标准化的Kafka消费配置
 */
public class KafkaConsumerConfig {
    
    /**
     * 创建基础消费者配置
     */
    public static Properties createBaseConfig(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        
        // 基础连接配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // 消费策略配置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 手动提交offset
        
        // 性能优化配置
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024"); // 1KB
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000"); // 5分钟
        
        // 会话配置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000"); // 30秒
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000"); // 10秒
        
        return props;
    }
    
    /**
     * 创建数据节点消费者配置
     */
    public static Properties createDataNodeConfig(String bootstrapServers, String nodeId) {
        Properties props = createBaseConfig(bootstrapServers, "data-node-" + nodeId);
        
        // 数据节点特定配置
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "10485760"); // 10MB
        
        return props;
    }
    
    /**
     * 创建查询节点消费者配置
     */
    public static Properties createQueryNodeConfig(String bootstrapServers, String nodeId) {
        Properties props = createBaseConfig(bootstrapServers, "query-node-" + nodeId);
        
        // 查询节点特定配置 - 更快的响应时间
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        
        return props;
    }
    
    /**
     * 创建协调器消费者配置
     */
    public static Properties createCoordinatorConfig(String bootstrapServers, String coordinatorType) {
        Properties props = createBaseConfig(bootstrapServers, coordinatorType + "-coordinator");
        
        // 协调器特定配置 - 高可靠性
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000"); // 60秒
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000"); // 10分钟
        
        return props;
    }
}