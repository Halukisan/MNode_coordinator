package com.milvus.node.data;

import com.milvus.storage.KafkaManager;
import com.milvus.storage.EtcdManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Collections;
import java.time.Duration;

/**
 * Data Node 服务器
 * 负责消费Kafka日志、维护数据缓冲区、执行Flush操作
 */
public class DataNodeServer {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeServer.class);
    
    private final String nodeId;
    private final String address;
    private final int port;
    private final KafkaManager kafkaManager;
    private final EtcdManager etcdManager;
    private final SegmentFlushManager flushManager;
    private final ScheduledExecutorService scheduler;
    
    // 数据缓冲区管理
    private final Map<String, SegmentBuffer> segmentBuffers = new ConcurrentHashMap<>();
    private final Map<String, Long> segmentSizes = new ConcurrentHashMap<>();
    
    // 配置参数
    private final long maxBufferSize = 256 * 1024 * 1024; // 256MB
    private final int maxBufferRecords = 100000;
    private final long flushInterval = 30000; // 30秒
    
    // Kafka消费者
    private volatile KafkaConsumer<String, String> insertLogConsumer;
    private volatile KafkaConsumer<String, String> deleteLogConsumer;
    
    // 消费者线程控制
    private volatile boolean consumersRunning = false;
    
    public DataNodeServer(String nodeId, String address, int port, 
                         String kafkaServers, String etcdEndpoints) {
        this.nodeId = nodeId;
        this.address = address;
        this.port = port;
        this.kafkaManager = new KafkaManager(kafkaServers);
        this.etcdManager = new EtcdManager(etcdEndpoints);
        this.flushManager = new SegmentFlushManager(nodeId);
        this.scheduler = Executors.newScheduledThreadPool(4);
        
        logger.info("DataNode initialized: id={}, address={}:{}", nodeId, address, port);
    }
    
    /**
     * 启动服务
     */
    public void start() {
        try {
            // 注册节点到Etcd
            registerNode();
            
            // 启动Kafka消费者
            startKafkaConsumers();
            
            // 启动定时Flush
            startPeriodicFlush();
            
            // 启动健康检查
            startHealthCheck();
            
            // 启动负载报告
            startLoadReporting();
            
            logger.info("DataNode started successfully");
            
        } catch (Exception e) {
            logger.error("Failed to start DataNode", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 注册节点
     */
    private void registerNode() {
        EtcdManager.NodeMetadata nodeMetadata = new EtcdManager.NodeMetadata(
            nodeId, "data-node", address, port);
        
        etcdManager.registerNode("data", nodeId, nodeMetadata)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        logger.error("Failed to register data node", throwable);
                    } else {
                        logger.info("Data node registered successfully");
                    }
                });
    }
    
    /**
     * 启动Kafka消费者
     */
    private void startKafkaConsumers() {
        consumersRunning = true;
        
        // 启动插入日志消费者
        insertLogConsumer = kafkaManager.createDataNodeConsumer(nodeId + "-insert");
        insertLogConsumer.subscribe(Collections.singletonList("milvus-insert-log"));
        
        scheduler.submit(() -> {
            logger.info("Insert log consumer started");
            
            while (consumersRunning && !Thread.currentThread().isInterrupted()) {
                try {
                    if (insertLogConsumer != null) {
                        var records = insertLogConsumer.poll(Duration.ofMillis(1000));
                        
                        for (ConsumerRecord<String, String> record : records) {
                            processInsertLog(record);
                        }
                        
                        if (!records.isEmpty()) {
                            insertLogConsumer.commitSync();
                        }
                    }
                    
                } catch (Exception e) {
                    if (consumersRunning) {
                        logger.error("Error processing insert log", e);
                    }
                }
            }
            logger.info("Insert log consumer stopped");
        });
        
        // 启动删除日志消费者
        deleteLogConsumer = kafkaManager.createDataNodeConsumer(nodeId + "-delete");
        deleteLogConsumer.subscribe(Collections.singletonList("milvus-delete-log"));
        
        scheduler.submit(() -> {
            logger.info("Delete log consumer started");
            
            while (consumersRunning && !Thread.currentThread().isInterrupted()) {
                try {
                    if (deleteLogConsumer != null) {
                        var records = deleteLogConsumer.poll(Duration.ofMillis(1000));
                        
                        for (ConsumerRecord<String, String> record : records) {
                            processDeleteLog(record);
                        }
                        
                        if (!records.isEmpty()) {
                            deleteLogConsumer.commitSync();
                        }
                    }
                    
                } catch (Exception e) {
                    if (consumersRunning) {
                        logger.error("Error processing delete log", e);
                    }
                }
            }
            logger.info("Delete log consumer stopped");
        });
    }
    
    /**
     * 处理插入日志
     */
    private void processInsertLog(ConsumerRecord<String, String> record) {
        try {
            String segmentId = record.key();
            String data = record.value();
            
            // 获取或创建Segment缓冲区
            SegmentBuffer buffer = segmentBuffers.computeIfAbsent(segmentId, 
                k -> new SegmentBuffer(segmentId, maxBufferSize, maxBufferRecords));
            
            // 添加数据到缓冲区
            buffer.addRecord(data);
            
            // 更新Segment大小统计
            segmentSizes.put(segmentId, buffer.getCurrentSize());
            
            // 检查是否需要触发Flush
            if (buffer.shouldFlush()) {
                triggerFlush(segmentId);
            }
            
            logger.debug("Processed insert log: segment={}, size={}", segmentId, buffer.getCurrentSize());
            
        } catch (Exception e) {
            logger.error("Failed to process insert log: {}", record, e);
        }
    }
    
    /**
     * 处理删除日志
     */
    private void processDeleteLog(ConsumerRecord<String, String> record) {
        try {
            String segmentId = record.key();
            String deleteInfo = record.value();
            
            // 处理删除操作
            SegmentBuffer buffer = segmentBuffers.get(segmentId);
            if (buffer != null) {
                buffer.addDeleteRecord(deleteInfo);
                logger.debug("Processed delete log: segment={}", segmentId);
            } else {
                logger.warn("Segment buffer not found for delete: {}", segmentId);
            }
            
        } catch (Exception e) {
            logger.error("Failed to process delete log: {}", record, e);
        }
    }
    
    /**
     * 触发Segment Flush
     */
    public void triggerFlush(String segmentId) {
        SegmentBuffer buffer = segmentBuffers.get(segmentId);
        if (buffer == null) {
            logger.warn("No buffer found for segment: {}", segmentId);
            return;
        }
        
        if (buffer.isFlushInProgress()) {
            logger.debug("Flush already in progress for segment: {}", segmentId);
            return;
        }
        
        // 异步执行Flush
        scheduler.submit(() -> {
            try {
                buffer.setFlushInProgress(true);
                
                // 执行Flush操作
                flushManager.flushSegment(segmentId, buffer);
                
                // 清理缓冲区
                segmentBuffers.remove(segmentId);
                segmentSizes.remove(segmentId);
                
                logger.info("Segment flushed successfully: {}", segmentId);
                
            } catch (Exception e) {
                logger.error("Failed to flush segment: {}", segmentId, e);
            } finally {
                buffer.setFlushInProgress(false);
            }
        });
    }
    
    /**
     * 启动定时Flush
     */
    private void startPeriodicFlush() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                long currentTime = System.currentTimeMillis();
                
                segmentBuffers.entrySet().forEach(entry -> {
                    String segmentId = entry.getKey();
                    SegmentBuffer buffer = entry.getValue();
                    
                    // 检查缓冲区是否超时
                    if (currentTime - buffer.getLastUpdateTime() >= flushInterval) {
                        triggerFlush(segmentId);
                    }
                });
                
            } catch (Exception e) {
                logger.error("Periodic flush failed", e);
            }
        }, flushInterval, flushInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 启动健康检查
     */
    private void startHealthCheck() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // 检查节点健康状态
                boolean isHealthy = checkNodeHealth();
                
                if (!isHealthy) {
                    logger.warn("Node health check failed");
                }
                
            } catch (Exception e) {
                logger.error("Health check failed", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
    
    /**
     * 启动负载报告
     */
    private void startLoadReporting() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // 计算当前负载
                int currentLoad = segmentBuffers.size();
                int memoryUsage = calculateMemoryUsage();
                int cpuUsage = calculateCpuUsage();
                
                // 报告负载到协调器（这里简化为日志输出）
                logger.debug("Node load report: segments={}, memory={}%, cpu={}%", 
                           currentLoad, memoryUsage, cpuUsage);
                
            } catch (Exception e) {
                logger.error("Load reporting failed", e);
            }
        }, 10, 10, TimeUnit.SECONDS);
    }
    
    /**
     * 检查节点健康状态
     */
    private boolean checkNodeHealth() {
        // 检查Kafka连接
        if (insertLogConsumer == null || deleteLogConsumer == null) {
            return false;
        }
        
        // 检查内存使用
        long totalMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();
        double memoryUsage = (double) (totalMemory - freeMemory) / totalMemory;
        
        if (memoryUsage > 0.9) { // 内存使用超过90%
            return false;
        }
        
        return true;
    }
    
    /**
     * 计算内存使用率
     */
    private int calculateMemoryUsage() {
        long totalMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();
        return (int) ((totalMemory - freeMemory) * 100 / totalMemory);
    }
    
    /**
     * 计算CPU使用率（简化实现）
     */
    private int calculateCpuUsage() {
        // 简化实现，实际应该使用系统监控API
        return (int) (Math.random() * 50); // 模拟0-50%的CPU使用率
    }
    
    /**
     * 获取节点状态信息
     */
    public String getNodeStatus() {
        return String.format("DataNode{id=%s, segments=%d, totalSize=%d}", 
                           nodeId, segmentBuffers.size(), 
                           segmentSizes.values().stream().mapToLong(Long::longValue).sum());
    }
    
    /**
     * 关闭服务
     */
    public void shutdown() {
        try {
            logger.info("Starting DataNode shutdown...");
            
            // 首先停止消费者循环
            consumersRunning = false;
            
            // 等待消费者线程停止
            Thread.sleep(2000);
            
            // 关闭调度器，中断所有正在运行的任务
            scheduler.shutdown();
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Scheduler did not terminate gracefully, forcing shutdown");
                scheduler.shutdownNow();
                // 等待强制关闭完成
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.error("Scheduler did not terminate after forced shutdown");
                }
            }
            
            // 现在安全地关闭Kafka消费者
            try {
                if (insertLogConsumer != null) {
                    insertLogConsumer.close(Duration.ofSeconds(5));
                    logger.info("Insert log consumer closed");
                }
            } catch (Exception e) {
                logger.warn("Error closing insert log consumer", e);
            }
            
            try {
                if (deleteLogConsumer != null) {
                    deleteLogConsumer.close(Duration.ofSeconds(5));
                    logger.info("Delete log consumer closed");
                }
            } catch (Exception e) {
                logger.warn("Error closing delete log consumer", e);
            }
            
            // 关闭其他组件
            try {
                flushManager.shutdown();
            } catch (Exception e) {
                logger.warn("Error shutting down flush manager", e);
            }
            
            try {
                kafkaManager.shutdown();
            } catch (Exception e) {
                logger.warn("Error shutting down kafka manager", e);
            }
            
            try {
                // 注销节点
                etcdManager.unregisterNode("data", nodeId);
                etcdManager.close();
            } catch (Exception e) {
                logger.warn("Error closing etcd manager", e);
            }
            
            logger.info("DataNode shutdown completed");
            
        } catch (Exception e) {
            logger.error("Error during shutdown", e);
        }
    }
}