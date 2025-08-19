package com.milvus.storage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KafkaManager 测试用例
 * 注意：需要启动Kafka服务才能运行这些测试
 */
public class KafkaManagerTest {
    
    private KafkaManager kafkaManager;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TEST_TOPIC = "test-topic";
    
    @BeforeEach
    public void setUp() {
        kafkaManager = new KafkaManager(BOOTSTRAP_SERVERS);
    }
    
    @AfterEach
    public void tearDown() {
        if (kafkaManager != null) {
            kafkaManager.shutdown();
        }
    }
    
    @Test
    public void testSendMessage() throws Exception {
        String key = "test-key";
        String value = "test-value";
        
        CompletableFuture<RecordMetadata> future = kafkaManager.sendMessage(TEST_TOPIC, key, value);
        RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
        
        assertNotNull(metadata);
        assertEquals(TEST_TOPIC, metadata.topic());
        assertTrue(metadata.offset() >= 0);
    }
    
    @Test
    public void testSendMessageSync() {
        String key = "sync-key";
        String value = "sync-value";
        
        RecordMetadata metadata = kafkaManager.sendMessageSync(TEST_TOPIC, key, value);
        
        assertNotNull(metadata);
        assertEquals(TEST_TOPIC, metadata.topic());
        assertTrue(metadata.offset() >= 0);
    }
    
    @Test
    public void testSendMessageBatch() {
        List<KafkaManager.MessageRecord> messages = new ArrayList<>();
        
        for (int i = 0; i < 10; i++) {
            messages.add(new KafkaManager.MessageRecord("key" + i, "value" + i));
        }
        
        assertDoesNotThrow(() -> {
            kafkaManager.sendMessageBatch(TEST_TOPIC, messages);
        });
    }
    
    @Test
    public void testCreateConsumer() {
        String groupId = "test-group";
        String nodeId = "test-node";
        
        KafkaConsumer<String, String> consumer = kafkaManager.createConsumer(groupId, nodeId);
        
        assertNotNull(consumer);
        consumer.close();
    }
    
    @Test
    public void testCreateDataNodeConsumer() {
        String nodeId = "data-node-1";
        
        KafkaConsumer<String, String> consumer = kafkaManager.createDataNodeConsumer(nodeId);
        
        assertNotNull(consumer);
        consumer.close();
    }
    
    @Test
    public void testCreateQueryNodeConsumer() {
        String nodeId = "query-node-1";
        
        KafkaConsumer<String, String> consumer = kafkaManager.createQueryNodeConsumer(nodeId);
        
        assertNotNull(consumer);
        consumer.close();
    }
    
    @Test
    public void testProducerConsumerIntegration() throws Exception {
        String groupId = "integration-test-group";
        String nodeId = "integration-test-node";
        String testKey = "integration-key";
        String testValue = "integration-value";
        
        // 发送消息
        CompletableFuture<RecordMetadata> sendFuture = kafkaManager.sendMessage(TEST_TOPIC, testKey, testValue);
        RecordMetadata metadata = sendFuture.get(10, TimeUnit.SECONDS);
        assertNotNull(metadata);
        
        // 创建消费者并消费消息
        KafkaConsumer<String, String> consumer = kafkaManager.createConsumer(groupId, nodeId);
        consumer.subscribe(Collections.singletonList(TEST_TOPIC));
        
        // 轮询消息
        boolean messageReceived = false;
        int maxAttempts = 10;
        int attempts = 0;
        
        while (!messageReceived && attempts < maxAttempts) {
            var records = consumer.poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, String> record : records) {
                if (testKey.equals(record.key()) && testValue.equals(record.value())) {
                    messageReceived = true;
                    assertEquals(TEST_TOPIC, record.topic());
                    break;
                }
            }
            attempts++;
        }
        
        consumer.close();
        assertTrue(messageReceived, "Message should have been received by consumer");
    }
    
    @Test
    public void testStartConsumerAsync() throws Exception {
        String groupId = "async-test-group";
        String nodeId = "async-test-node";
        String testKey = "async-key";
        String testValue = "async-value";
        
        AtomicInteger messageCount = new AtomicInteger(0);
        
        // 启动异步消费者
        kafkaManager.startConsumer(TEST_TOPIC, groupId, nodeId, record -> {
            if (testKey.equals(record.key()) && testValue.equals(record.value())) {
                messageCount.incrementAndGet();
            }
        });
        
        // 等待消费者启动
        Thread.sleep(2000);
        
        // 发送测试消息
        kafkaManager.sendMessage(TEST_TOPIC, testKey, testValue).get(5, TimeUnit.SECONDS);
        
        // 等待消息被消费
        Thread.sleep(3000);
        
        assertTrue(messageCount.get() > 0, "Consumer should have received the message");
    }
    
    @Test
    public void testGetTopicInfo() {
        assertDoesNotThrow(() -> {
            kafkaManager.getTopicInfo(TEST_TOPIC);
        });
    }
    
    @Test
    public void testConcurrentProducing() throws Exception {
        int numMessages = 100;
        List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
        
        // 并发发送消息
        for (int i = 0; i < numMessages; i++) {
            String key = "concurrent-key-" + i;
            String value = "concurrent-value-" + i;
            futures.add(kafkaManager.sendMessage(TEST_TOPIC, key, value));
        }
        
        // 等待所有消息发送完成
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futures.toArray(new CompletableFuture[0])
        );
        
        allFutures.get(30, TimeUnit.SECONDS);
        
        // 验证所有消息都发送成功
        for (CompletableFuture<RecordMetadata> future : futures) {
            RecordMetadata metadata = future.get();
            assertNotNull(metadata);
            assertEquals(TEST_TOPIC, metadata.topic());
        }
    }
    
    @Test
    public void testMessageRecordClass() {
        String key = "test-key";
        String value = "test-value";
        
        KafkaManager.MessageRecord record = new KafkaManager.MessageRecord(key, value);
        
        assertEquals(key, record.getKey());
        assertEquals(value, record.getValue());
    }
}