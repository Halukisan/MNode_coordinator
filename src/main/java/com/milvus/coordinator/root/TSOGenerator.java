package com.milvus.coordinator.root;

import com.milvus.common.TSO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TSO 生成器
 * 负责生成全局唯一、单调递增的时间戳
 */
public class TSOGenerator {
    private static final Logger logger = LoggerFactory.getLogger(TSOGenerator.class);
    
    private final ReentrantLock lock = new ReentrantLock();
    private volatile long lastPhysical = 0;
    private final AtomicLong logicalCounter = new AtomicLong(0);
    
    // TSO批量分配大小，减少锁竞争
    private static final int BATCH_SIZE = 1000;
    private volatile long batchStart = 0;
    private volatile long batchEnd = 0;
    
    /**
     * 生成单个TSO
     */
    public TSO generateTSO() {
        long currentTime = System.currentTimeMillis();
        
        lock.lock();
        try {
            // 如果物理时间前进，重置逻辑计数器
            if (currentTime > lastPhysical) {
                lastPhysical = currentTime;
                logicalCounter.set(0);
                return new TSO(currentTime, 0);
            }
            
            // 物理时间相同，递增逻辑计数器
            long logical = logicalCounter.incrementAndGet();
            
            // 检查逻辑计数器是否溢出（22位最大值）
            if (logical > 0x3FFFFF) {
                // 等待下一毫秒
                try {
                    Thread.sleep(1);
                    return generateTSO();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("TSO generation interrupted", e);
                }
            }
            
            return new TSO(lastPhysical, logical);
            
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * 批量生成TSO，提高性能
     */
    public TSO[] generateTSOBatch(int count) {
        if (count <= 0 || count > BATCH_SIZE) {
            throw new IllegalArgumentException("Batch size must be between 1 and " + BATCH_SIZE);
        }
        
        TSO[] batch = new TSO[count];
        long currentTime = System.currentTimeMillis();
        
        lock.lock();
        try {
            if (currentTime > lastPhysical) {
                lastPhysical = currentTime;
                logicalCounter.set(0);
            }
            
            long startLogical = logicalCounter.get();
            
            // 检查是否会溢出
            if (startLogical + count > 0x3FFFFF) {
                // 等待下一毫秒重试
                try {
                    Thread.sleep(1);
                    return generateTSOBatch(count);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("TSO batch generation interrupted", e);
                }
            }
            
            // 生成批量TSO
            for (int i = 0; i < count; i++) {
                long logical = logicalCounter.incrementAndGet();
                batch[i] = new TSO(lastPhysical, logical);
            }
            
            logger.debug("Generated TSO batch: count={}, physical={}, logical={}-{}", 
                        count, lastPhysical, startLogical + 1, startLogical + count);
            
            return batch;
            
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * 获取当前TSO状态信息
     */
    public String getStatus() {
        return String.format("TSOGenerator{lastPhysical=%d, logicalCounter=%d}", 
                           lastPhysical, logicalCounter.get());
    }
}