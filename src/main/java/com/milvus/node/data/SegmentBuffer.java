package com.milvus.node.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.List;
import java.util.ArrayList;

/**
 * Segment 数据缓冲区
 * 用于在内存中缓存数据，直到达到Flush条件
 */
public class SegmentBuffer {
    private static final Logger logger = LoggerFactory.getLogger(SegmentBuffer.class);
    
    private final String segmentId;
    private final long maxSize;
    private final int maxRecords;
    
    // 数据存储
    private final ConcurrentLinkedQueue<String> records = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> deleteRecords = new ConcurrentLinkedQueue<>();
    
    // 统计信息
    private final AtomicLong currentSize = new AtomicLong(0);
    private final AtomicLong recordCount = new AtomicLong(0);
    private final AtomicLong deleteCount = new AtomicLong(0);
    private volatile long createTime;
    private volatile long lastUpdateTime;
    
    // 状态管理
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);
    private final AtomicBoolean sealed = new AtomicBoolean(false);
    
    public SegmentBuffer(String segmentId, long maxSize, int maxRecords) {
        this.segmentId = segmentId;
        this.maxSize = maxSize;
        this.maxRecords = maxRecords;
        this.createTime = System.currentTimeMillis();
        this.lastUpdateTime = createTime;
        
        logger.debug("SegmentBuffer created: id={}, maxSize={}, maxRecords={}", 
                    segmentId, maxSize, maxRecords);
    }
    
    /**
     * 添加数据记录
     */
    public boolean addRecord(String record) {
        if (sealed.get()) {
            logger.warn("Cannot add record to sealed segment: {}", segmentId);
            return false;
        }
        
        if (flushInProgress.get()) {
            logger.debug("Flush in progress, rejecting new record for segment: {}", segmentId);
            return false;
        }
        
        // 检查容量限制
        if (recordCount.get() >= maxRecords) {
            logger.debug("Segment buffer full (records): {}, count={}", segmentId, recordCount.get());
            return false;
        }
        
        long recordSize = record.length();
        if (currentSize.get() + recordSize > maxSize) {
            logger.debug("Segment buffer full (size): {}, current={}, adding={}", 
                        segmentId, currentSize.get(), recordSize);
            return false;
        }
        
        // 添加记录
        records.offer(record);
        currentSize.addAndGet(recordSize);
        recordCount.incrementAndGet();
        lastUpdateTime = System.currentTimeMillis();
        
        logger.trace("Record added to segment: {}, size={}, count={}", 
                    segmentId, currentSize.get(), recordCount.get());
        
        return true;
    }
    
    /**
     * 添加删除记录
     */
    public boolean addDeleteRecord(String deleteRecord) {
        if (sealed.get()) {
            logger.warn("Cannot add delete record to sealed segment: {}", segmentId);
            return false;
        }
        
        deleteRecords.offer(deleteRecord);
        deleteCount.incrementAndGet();
        lastUpdateTime = System.currentTimeMillis();
        
        logger.trace("Delete record added to segment: {}, deleteCount={}", 
                    segmentId, deleteCount.get());
        
        return true;
    }
    
    /**
     * 检查是否应该触发Flush
     */
    public boolean shouldFlush() {
        // 检查记录数量
        if (recordCount.get() >= maxRecords) {
            logger.debug("Should flush due to record count: segment={}, count={}", 
                        segmentId, recordCount.get());
            return true;
        }
        
        // 检查数据大小
        if (currentSize.get() >= maxSize) {
            logger.debug("Should flush due to size: segment={}, size={}", 
                        segmentId, currentSize.get());
            return true;
        }
        
        return false;
    }
    
    /**
     * 检查是否应该基于时间触发Flush
     */
    public boolean shouldFlushByTime(long flushInterval) {
        long timeSinceLastUpdate = System.currentTimeMillis() - lastUpdateTime;
        boolean shouldFlush = timeSinceLastUpdate >= flushInterval && recordCount.get() > 0;
        
        if (shouldFlush) {
            logger.debug("Should flush due to time: segment={}, timeSinceUpdate={}ms", 
                        segmentId, timeSinceLastUpdate);
        }
        
        return shouldFlush;
    }
    
    /**
     * 获取所有记录（用于Flush）
     */
    public List<String> getAllRecords() {
        List<String> allRecords = new ArrayList<>();
        
        String record;
        while ((record = records.poll()) != null) {
            allRecords.add(record);
        }
        
        logger.debug("Retrieved {} records from segment buffer: {}", allRecords.size(), segmentId);
        return allRecords;
    }
    
    /**
     * 获取所有删除记录（用于Flush）
     */
    public List<String> getAllDeleteRecords() {
        List<String> allDeleteRecords = new ArrayList<>();
        
        String deleteRecord;
        while ((deleteRecord = deleteRecords.poll()) != null) {
            allDeleteRecords.add(deleteRecord);
        }
        
        logger.debug("Retrieved {} delete records from segment buffer: {}", 
                    allDeleteRecords.size(), segmentId);
        return allDeleteRecords;
    }
    
    /**
     * 清空缓冲区
     */
    public void clear() {
        records.clear();
        deleteRecords.clear();
        currentSize.set(0);
        recordCount.set(0);
        deleteCount.set(0);
        
        logger.debug("Segment buffer cleared: {}", segmentId);
    }
    
    /**
     * 封存缓冲区（不再接受新数据）
     */
    public void seal() {
        sealed.set(true);
        logger.info("Segment buffer sealed: {}", segmentId);
    }
    
    /**
     * 获取缓冲区统计信息
     */
    public BufferStats getStats() {
        return new BufferStats(
            segmentId,
            recordCount.get(),
            deleteCount.get(),
            currentSize.get(),
            createTime,
            lastUpdateTime,
            flushInProgress.get(),
            sealed.get()
        );
    }
    
    /**
     * 检查缓冲区是否为空
     */
    public boolean isEmpty() {
        return recordCount.get() == 0 && deleteCount.get() == 0;
    }
    
    /**
     * 获取负载比例
     */
    public double getLoadRatio() {
        double sizeRatio = (double) currentSize.get() / maxSize;
        double recordRatio = (double) recordCount.get() / maxRecords;
        return Math.max(sizeRatio, recordRatio);
    }
    
    // Getters
    public String getSegmentId() { return segmentId; }
    public long getCurrentSize() { return currentSize.get(); }
    public long getRecordCount() { return recordCount.get(); }
    public long getDeleteCount() { return deleteCount.get(); }
    public long getCreateTime() { return createTime; }
    public long getLastUpdateTime() { return lastUpdateTime; }
    public boolean isFlushInProgress() { return flushInProgress.get(); }
    public boolean isSealed() { return sealed.get(); }
    public long getMaxSize() { return maxSize; }
    public int getMaxRecords() { return maxRecords; }
    
    // Setters for state management
    public void setFlushInProgress(boolean inProgress) { 
        flushInProgress.set(inProgress);
        if (inProgress) {
            logger.debug("Flush started for segment: {}", segmentId);
        } else {
            logger.debug("Flush completed for segment: {}", segmentId);
        }
    }
    
    @Override
    public String toString() {
        return String.format("SegmentBuffer{id='%s', records=%d, size=%d, sealed=%s, flushing=%s}",
                           segmentId, recordCount.get(), currentSize.get(), sealed.get(), flushInProgress.get());
    }
    
    /**
     * 缓冲区统计信息类
     */
    public static class BufferStats {
        private final String segmentId;
        private final long recordCount;
        private final long deleteCount;
        private final long currentSize;
        private final long createTime;
        private final long lastUpdateTime;
        private final boolean flushInProgress;
        private final boolean sealed;
        
        public BufferStats(String segmentId, long recordCount, long deleteCount, long currentSize,
                          long createTime, long lastUpdateTime, boolean flushInProgress, boolean sealed) {
            this.segmentId = segmentId;
            this.recordCount = recordCount;
            this.deleteCount = deleteCount;
            this.currentSize = currentSize;
            this.createTime = createTime;
            this.lastUpdateTime = lastUpdateTime;
            this.flushInProgress = flushInProgress;
            this.sealed = sealed;
        }
        
        // Getters
        public String getSegmentId() { return segmentId; }
        public long getRecordCount() { return recordCount; }
        public long getDeleteCount() { return deleteCount; }
        public long getCurrentSize() { return currentSize; }
        public long getCreateTime() { return createTime; }
        public long getLastUpdateTime() { return lastUpdateTime; }
        public boolean isFlushInProgress() { return flushInProgress; }
        public boolean isSealed() { return sealed; }
        
        @Override
        public String toString() {
            return String.format("BufferStats{segment='%s', records=%d, deletes=%d, size=%d, sealed=%s, flushing=%s}",
                               segmentId, recordCount, deleteCount, currentSize, sealed, flushInProgress);
        }
    }
}