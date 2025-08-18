package com.milvus.storage;

import com.milvus.common.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Kafka 管理器
 * 封装Kafka生产者和消费者操作
 */
public class KafkaManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaManager.class);
    
    private final String bootstrapServers;
    private KafkaProducer<String, String> producer;
    private ExecutorService consumerExecutor;
    
    public KafkaManager(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.consumerExecutor = Executors.newCachedThreadPool();
        initializeProducer();
        
        logger.info("KafkaManager initialized with servers: {}", bootstrapServers);
    }
    
    /**
     * 初始化生产者
     */
    private void initializeProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 性能和可靠性配置
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 等待所有副本确认
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // 16KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10"); // 10ms延迟批处理
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"); // 32MB缓冲区
        
        // 幂等性配置
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        
        this.producer = new KafkaProducer<>(props);
        logger.info("Kafka producer initialized");
    }
    
    /**
     * 发送消息（异步）
     */
    public CompletableFuture<RecordMetadata> sendMessage(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send message to topic {}: {}", topic, exception.getMessage());
                future.completeExceptionally(exception);
            } else {
                logger.debug("Message sent to topic {}, partition {}, offset {}", 
                           metadata.topic(), metadata.partition(), metadata.offset());
                future.complete(metadata);
            }
        });
        
        return future;
    }
    
    /**
     * 发送消息（同步）
     */
    public RecordMetadata sendMessageSync(String topic, String key, String value) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            RecordMetadata metadata = producer.send(record).get();
            
            logger.debug("Message sent synchronously to topic {}, partition {}, offset {}", 
                       metadata.topic(), metadata.partition(), metadata.offset());
            
            return metadata;
        } catch (Exception e) {
            logger.error("Failed to send message synchronously to topic {}", topic, e);
            throw new RuntimeException("Failed to send message", e);
        }
    }
    
    /**
     * 批量发送消息
     */
    public void sendMessageBatch(String topic, java.util.List<MessageRecord> messages) {
        for (MessageRecord message : messages) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.getKey(), message.getValue());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send batch message: {}", exception.getMessage());
                } else {
                    logger.debug("Batch message sent: partition {}, offset {}", 
                               metadata.partition(), metadata.offset());
                }
            });
        }
        
        // 强制发送所有批量消息
        producer.flush();
        logger.info("Sent {} messages in batch to topic {}", messages.size(), topic);
    }
    
    /**
     * 创建消费者
     */
    public KafkaConsumer<String, String> createConsumer(String groupId, String nodeId) {
        Properties props = KafkaConsumerConfig.createBaseConfig(bootstrapServers, groupId);
        props.put("client.id", nodeId);
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        logger.info("Kafka consumer created: groupId={}, nodeId={}", groupId, nodeId);
        
        return consumer;
    }
    
    /**
     * 启动消费者（异步）
     */
    public void startConsumer(String topic, String groupId, String nodeId, 
                             Consumer<ConsumerRecord<String, String>> messageHandler) {
        
        consumerExecutor.submit(() -> {
            KafkaConsumer<String, String> consumer = createConsumer(groupId, nodeId);
            
            try {
                consumer.subscribe(Collections.singletonList(topic));
                logger.info("Consumer started for topic: {}, groupId: {}", topic, groupId);
                
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            messageHandler.accept(record);
                            
                            // 手动提交offset
                            consumer.commitSync();
                            
                        } catch (Exception e) {
                            logger.error("Error processing message from topic {}: {}", topic, e.getMessage(), e);
                        }
                    }
                }
                
            } catch (Exception e) {
                logger.error("Consumer error for topic {}: {}", topic, e.getMessage(), e);
            } finally {
                try {
                    consumer.close();
                    logger.info("Consumer closed for topic: {}", topic);
                } catch (Exception e) {
                    logger.error("Error closing consumer", e);
                }
            }
        });
    }
    
    /**
     * 创建数据节点消费者
     */
    public KafkaConsumer<String, String> createDataNodeConsumer(String nodeId) {
        Properties props = KafkaConsumerConfig.createDataNodeConfig(bootstrapServers, nodeId);
        return new KafkaConsumer<>(props);
    }
    
    /**
     * 创建查询节点消费者
     */
    public KafkaConsumer<String, String> createQueryNodeConsumer(String nodeId) {
        Properties props = KafkaConsumerConfig.createQueryNodeConfig(bootstrapServers, nodeId);
        return new KafkaConsumer<>(props);
    }
    
    /**
     * 获取主题分区信息
     */
    public void getTopicInfo(String topic) {
        try (KafkaConsumer<String, String> consumer = createConsumer("info-group", "info-client")) {
            consumer.partitionsFor(topic).forEach(partitionInfo -> {
                logger.info("Topic: {}, Partition: {}, Leader: {}", 
                          partitionInfo.topic(), partitionInfo.partition(), partitionInfo.leader());
            });
        } catch (Exception e) {
            logger.error("Failed to get topic info for {}", topic, e);
        }
    }
    
    /**
     * 关闭管理器
     */
    public void shutdown() {
        try {
            if (producer != null) {
                producer.close();
                logger.info("Kafka producer closed");
            }
            
            consumerExecutor.shutdown();
            if (!consumerExecutor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                consumerExecutor.shutdownNow();
            }
            logger.info("Kafka consumer executor shutdown");
            
        } catch (Exception e) {
            logger.error("Error during KafkaManager shutdown", e);
        }
    }
    
    /**
     * 消息记录类
     */
    public static class MessageRecord {
        private final String key;
        private final String value;
        
        public MessageRecord(String key, String value) {
            this.key = key;
            this.value = value;
        }
        
        public String getKey() { return key; }
        public String getValue() { return value; }
    }
}