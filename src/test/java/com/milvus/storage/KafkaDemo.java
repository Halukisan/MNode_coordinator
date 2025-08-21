package com.milvus.storage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.time.Duration;
import java.util.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaDemo {
    private final static Logger logger = LoggerFactory.getLogger(KafkaDemo.class);

    private KafkaManager kafkaManager;
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
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
        String key = "sync-key";
        String value = "sync-value";

        RecordMetadata recordMetadata = kafkaManager.sendMessageSync(TEST_TOPIC, key, value);
        logger.info("~(￣▽￣)~*----->RecordMetadata: {}", recordMetadata);
    }


    @Test
    public void testSendMessageBatch() {
        List<KafkaManager.MessageRecord> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(new KafkaManager.MessageRecord("key" + i, "value" + i));
        }
        kafkaManager.sendMessageBatch(TEST_TOPIC, messages);
    }

    @Test
    public void testCreateConsumer() {
        String groupId = "test-group";
        String nodeId = "test-node";

        KafkaConsumer<String, String> consumer = kafkaManager.createConsumer(groupId, nodeId);

        logger.info("~(￣▽￣)~*Consumer created: {}", consumer);
    }

    @Test
    public void testCreateDataNodeConsumer() {
        String nodeId = "data-node-1";
        //创建数据节点消费者
        KafkaConsumer<String, String> consumer = kafkaManager.createDataNodeConsumer(nodeId);

        logger.info("~(￣▽￣)~*-->Consumer created: {}", consumer);
    }

    @Test
    public void testCreateQueryNodeConsumer() {
        String nodeId = "query-node-1";
        KafkaConsumer<String, String> queryNodeConsumer = kafkaManager.createQueryNodeConsumer(nodeId);

        logger.info("~(￣▽￣)~*-->Consumer created: {}", queryNodeConsumer);
    }


    @Test
    public void testProducerConsumerIntegration() throws ExecutionException, InterruptedException, TimeoutException {
        String groupId = "integration-test-group";
        String nodeId = "integration-test-node";
        String testKey = "integration-key";
        String testValue = "integration-value";

        //发送消息
        CompletableFuture<RecordMetadata> sendFuture = kafkaManager.sendMessage(TEST_TOPIC, testKey, testValue);
        RecordMetadata metadata = sendFuture.get(10, TimeUnit.SECONDS);
        logger.info("~(￣▽￣)~*-->Message sent to topic {}, partition {}, offset {}", metadata.topic(), metadata.partition(), metadata.offset());

        //创建消费者并消费消息
        KafkaConsumer<String,String>consumer = kafkaManager.createConsumer(groupId,nodeId);
        consumer.subscribe(Collections.singletonList(TEST_TOPIC));

        //轮询消息
        boolean messageReceived = false;
        int maxAttempts = 10;
        int attempt = 0;

        while(!messageReceived && attempt < maxAttempts){
            var records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record:records){
                if (testKey.equals(record.key())&&testValue.equals(record.value())){
                    messageReceived  = true;
                    logger.info("~(￣▽￣)~*-->Received message: key {}, value {}", record.key(), record.value());
                    break;
                }
            }
            attempt++;
        }
        consumer.close();
        logger.info("~(￣▽￣)~*-->Message received: {}", messageReceived);
    }

    @Test
    public void testStartConsumerAsync() throws Exception{
        String groupId = "async-test-group";
        String nodeId = "async-test-node";
        String testKey = "async-key";
        String testValue = "async-value";

        AtomicInteger messageCount = new AtomicInteger(0);
        //启动异步消费者
        kafkaManager.startConsumer(TEST_TOPIC,groupId,nodeId,record->{
            if (testKey.equals(record.key())&& testValue.equals(record.value())){
                messageCount.incrementAndGet();
            }
        });

        //等待消费者启动
        Thread.sleep(2000);
        kafkaManager.sendMessage(TEST_TOPIC,testKey,testValue).get(5,TimeUnit.SECONDS);

        //等待消息被消费
        Thread.sleep(3000);

        logger.info("~(￣▽￣)~*-->Message count: {}", messageCount.get());
    }

    @Test
    public void testGetTopicInfo(){
        kafkaManager.getTopicInfo(TEST_TOPIC);
    }

    @Test
    public void testConcurrentProducing() throws ExecutionException, InterruptedException, TimeoutException {
        String groupId = "integration-test-group";
        String nodeId = "integration-test-node";
        int numMessages = 100;
        List<CompletableFuture<RecordMetadata>>futures = new ArrayList<>();

        //并发发送消息
        for(int i = 0;i<numMessages;i++){
            String key = "concurrent-key-"+i;
            String value = "concurrent-value-"+i;
            futures.add(kafkaManager.sendMessage(TEST_TOPIC,key,value));
        }

        //等待所有的消息发送完成
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        allFutures.get(30,TimeUnit.SECONDS);

        //验证所有消息都发送成功
        for(CompletableFuture<RecordMetadata> future:futures){
            RecordMetadata metadata = future.get();
            logger.info("~(￣▽￣)~*-->Message sent to topic {}, partition {}, offset {}", metadata.topic(), metadata.partition(), metadata.offset());
        }


        //启动消费者
        KafkaConsumer<String, String> consumer = kafkaManager.createConsumer(groupId, nodeId);
        consumer.subscribe(Collections.singletonList(TEST_TOPIC));

        Set<String> receivedKeys = new HashSet<>(); // 记录收到的 key
        int totalExpected = numMessages;
        long pollTimeoutMs = 1000;
        int maxPollAttempts = 30; // 最大等待时间 = 30 * 1s = 30s
        int pollAttempt = 0;

        while (receivedKeys.size() < totalExpected && pollAttempt < maxPollAttempts) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
            pollAttempt++;

            if (records.isEmpty()) {
                logger.info("Poll attempt {}:no records received",pollAttempt);
                continue;
            }
            for (ConsumerRecord<String,String> record : records){
                String key = record.key();
                String value = record.value();

                if (key!=null && key.matches("concurrent-key-\\d+")){
                    int index = Integer.parseInt(key.substring("concurrent-key-".length()));
                    String expectedValue = "concurrent-value-" + index;
                    if (index>=0 && index<numMessages && expectedValue.equals(value)){
                        boolean isNew = receivedKeys.add(key);
                        if (isNew){
                            logger.info("~(￣▽￣)~*--> Received message: key {}, value {}", record.key(), record.value());
                        }
                    }else{
                        logger.warn("Invalid message received: key {}, value {}", record.key(), record.value());
                    }
                }else{
                    logger.warn("Invalid message received: key {}, value {}", record.key(), record.value());
                }
            }
        }

        consumer.close();

// 验证结果
        logger.info("~(￣▽￣)~*--> Total messages received: {}", receivedKeys.size());
        assertTrue(receivedKeys.size() == totalExpected, "Not all messages were consumed");

        for (int i = 0; i < numMessages; i++) {
            assertTrue(receivedKeys.contains("concurrent-key-" + i), "Missing message with key concurrent-key-" + i);
        }
    }
}
