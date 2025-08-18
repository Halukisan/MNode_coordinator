package com.milvus.node.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;

/**
 * Segment Flush 管理器
 * 负责将内存中的数据缓冲区刷写到持久化存储
 */
public class SegmentFlushManager {
    private static final Logger logger = LoggerFactory.getLogger(SegmentFlushManager.class);
    
    private final String nodeId;
    private final ExecutorService flushExecutor;
    private final AtomicLong flushCounter = new AtomicLong(0);
    
    // 配置参数
    private final int flushThreads = 4;
    private final long flushTimeoutMs = 30000; // 30秒超时
    
    public SegmentFlushManager(String nodeId) {
        this.nodeId = nodeId;
        this.flushExecutor = Executors.newFixedThreadPool(flushThreads);
        
        logger.info("SegmentFlushManager initialized: nodeId={}, threads={}", nodeId, flushThreads);
    }
    
    /**
     * 刷写Segment到持久化存储
     */
    public CompletableFuture<FlushResult> flushSegment(String segmentId, SegmentBuffer buffer) {
        long flushId = flushCounter.incrementAndGet();
        
        logger.info("Starting flush: segmentId={}, flushId={}, records={}, size={}", 
                   segmentId, flushId, buffer.getRecordCount(), buffer.getCurrentSize());
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                long startTime = System.currentTimeMillis();
                
                // 1. 准备Flush数据
                FlushData flushData = prepareFlushData(segmentId, buffer);
                
                // 2. 写入对象存储
                String storageLocation = writeToObjectStorage(segmentId, flushData);
                
                // 3. 更新元数据
                updateSegmentMetadata(segmentId, storageLocation, flushData);
                
                long duration = System.currentTimeMillis() - startTime;
                
                FlushResult result = new FlushResult(segmentId, flushId, storageLocation, 
                                                   flushData.getRecordCount(), flushData.getDataSize(), 
                                                   duration, true, null);
                
                logger.info("Flush completed: segmentId={}, flushId={}, duration={}ms, location={}", 
                           segmentId, flushId, duration, storageLocation);
                
                return result;
                
            } catch (Exception e) {
                logger.error("Flush failed: segmentId={}, flushId={}", segmentId, flushId, e);
                
                return new FlushResult(segmentId, flushId, null, 0, 0, 0, false, e.getMessage());
            }
            
        }, flushExecutor);
    }
    
    /**
     * 准备Flush数据
     */
    private FlushData prepareFlushData(String segmentId, SegmentBuffer buffer) {
        logger.debug("Preparing flush data for segment: {}", segmentId);
        
        // 获取所有记录
        List<String> records = buffer.getAllRecords();
        List<String> deleteRecords = buffer.getAllDeleteRecords();
        
        // 计算数据大小
        long dataSize = records.stream().mapToLong(String::length).sum();
        long deleteSize = deleteRecords.stream().mapToLong(String::length).sum();
        
        FlushData flushData = new FlushData(segmentId, records, deleteRecords, 
                                          dataSize + deleteSize, System.currentTimeMillis());
        
        logger.debug("Flush data prepared: segment={}, records={}, deletes={}, size={}", 
                    segmentId, records.size(), deleteRecords.size(), flushData.getDataSize());
        
        return flushData;
    }
    
    /**
     * 写入对象存储
     */
    private String writeToObjectStorage(String segmentId, FlushData flushData) {
        // 模拟写入对象存储的过程
        // 实际实现中应该调用MinIO或S3的API
        
        try {
            // 模拟写入延迟
            Thread.sleep(100 + (long) (Math.random() * 500));
            
            // 生成存储位置
            String storageLocation = String.format("s3://milvus-data/segments/%s/%s.parquet", 
                                                 segmentId.substring(0, 8), segmentId);
            
            logger.debug("Data written to object storage: segment={}, location={}", 
                        segmentId, storageLocation);
            
            return storageLocation;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Write to object storage interrupted", e);
        }
    }
    
    /**
     * 更新Segment元数据
     */
    private void updateSegmentMetadata(String segmentId, String storageLocation, FlushData flushData) {
        // 这里应该更新Etcd中的Segment元数据
        logger.debug("Updating segment metadata: segment={}, location={}", segmentId, storageLocation);
        
        // 模拟元数据更新
        // 实际实现中应该调用EtcdManager的API
    }
    
    /**
     * 批量Flush多个Segment
     */
    public CompletableFuture<List<FlushResult>> flushSegmentBatch(List<SegmentFlushRequest> requests) {
        logger.info("Starting batch flush: count={}", requests.size());
        
        List<CompletableFuture<FlushResult>> futures = new ArrayList<>();
        
        for (SegmentFlushRequest request : requests) {
            CompletableFuture<FlushResult> future = flushSegment(request.getSegmentId(), request.getBuffer());
            futures.add(future);
        }
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(java.util.stream.Collectors.toList()));
    }
    
    /**
     * 获取Flush统计信息
     */
    public FlushStats getFlushStats() {
        return new FlushStats(flushCounter.get(), flushExecutor.toString());
    }
    
    /**
     * 关闭管理器
     */
    public void shutdown() {
        try {
            flushExecutor.shutdown();
            if (!flushExecutor.awaitTermination(flushTimeoutMs, TimeUnit.MILLISECONDS)) {
                flushExecutor.shutdownNow();
            }
            
            logger.info("SegmentFlushManager shutdown completed");
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Shutdown interrupted", e);
        }
    }
    
    /**
     * Flush数据类
     */
    public static class FlushData {
        private final String segmentId;
        private final List<String> records;
        private final List<String> deleteRecords;
        private final long dataSize;
        private final long timestamp;
        
        public FlushData(String segmentId, List<String> records, List<String> deleteRecords, 
                        long dataSize, long timestamp) {
            this.segmentId = segmentId;
            this.records = new ArrayList<>(records);
            this.deleteRecords = new ArrayList<>(deleteRecords);
            this.dataSize = dataSize;
            this.timestamp = timestamp;
        }
        
        public String getSegmentId() { return segmentId; }
        public List<String> getRecords() { return records; }
        public List<String> getDeleteRecords() { return deleteRecords; }
        public long getDataSize() { return dataSize; }
        public long getTimestamp() { return timestamp; }
        public int getRecordCount() { return records.size(); }
    }
    
    /**
     * Flush结果类
     */
    public static class FlushResult {
        private final String segmentId;
        private final long flushId;
        private final String storageLocation;
        private final int recordCount;
        private final long dataSize;
        private final long duration;
        private final boolean success;
        private final String errorMessage;
        
        public FlushResult(String segmentId, long flushId, String storageLocation, 
                          int recordCount, long dataSize, long duration, 
                          boolean success, String errorMessage) {
            this.segmentId = segmentId;
            this.flushId = flushId;
            this.storageLocation = storageLocation;
            this.recordCount = recordCount;
            this.dataSize = dataSize;
            this.duration = duration;
            this.success = success;
            this.errorMessage = errorMessage;
        }
        
        // Getters
        public String getSegmentId() { return segmentId; }
        public long getFlushId() { return flushId; }
        public String getStorageLocation() { return storageLocation; }
        public int getRecordCount() { return recordCount; }
        public long getDataSize() { return dataSize; }
        public long getDuration() { return duration; }
        public boolean isSuccess() { return success; }
        public String getErrorMessage() { return errorMessage; }
        
        @Override
        public String toString() {
            return String.format("FlushResult{segmentId='%s', flushId=%d, success=%s, records=%d, size=%d, duration=%dms}",
                               segmentId, flushId, success, recordCount, dataSize, duration);
        }
    }
    
    /**
     * Segment Flush请求类
     */
    public static class SegmentFlushRequest {
        private final String segmentId;
        private final SegmentBuffer buffer;
        
        public SegmentFlushRequest(String segmentId, SegmentBuffer buffer) {
            this.segmentId = segmentId;
            this.buffer = buffer;
        }
        
        public String getSegmentId() { return segmentId; }
        public SegmentBuffer getBuffer() { return buffer; }
    }
    
    /**
     * Flush统计信息类
     */
    public static class FlushStats {
        private final long totalFlushCount;
        private final String executorInfo;
        
        public FlushStats(long totalFlushCount, String executorInfo) {
            this.totalFlushCount = totalFlushCount;
            this.executorInfo = executorInfo;
        }
        
        public long getTotalFlushCount() { return totalFlushCount; }
        public String getExecutorInfo() { return executorInfo; }
        
        @Override
        public String toString() {
            return String.format("FlushStats{totalFlushCount=%d, executor='%s'}", 
                               totalFlushCount, executorInfo);
        }
    }
}