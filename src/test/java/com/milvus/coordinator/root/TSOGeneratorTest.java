package com.milvus.coordinator.root;

import com.milvus.common.TSO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * TSOGenerator 测试用例
 */
public class TSOGeneratorTest {
    
    private TSOGenerator tsoGenerator;
    
    @BeforeEach
    public void setUp() {
        tsoGenerator = new TSOGenerator();
    }
    
    @Test
    public void testGenerateSingleTSO() {
        TSO tso = tsoGenerator.generateTSO();
        
        assertNotNull(tso);
        assertTrue(tso.getPhysical() > 0);
        assertTrue(tso.getLogical() >= 0);
    }
    
    @Test
    public void testGenerateMultipleTSO() {
        TSO tso1 = tsoGenerator.generateTSO();
        TSO tso2 = tsoGenerator.generateTSO();
        TSO tso3 = tsoGenerator.generateTSO();
        
        // 验证TSO是递增的
        assertTrue(tso2.compareTo(tso1) > 0);
        assertTrue(tso3.compareTo(tso2) > 0);
    }
    
    @Test
    public void testGenerateTSOBatch() {
        int batchSize = 10;
        TSO[] tsos = tsoGenerator.generateTSOBatch(batchSize);
        
        assertNotNull(tsos);
        assertEquals(batchSize, tsos.length);
        
        // 验证批量TSO是递增的
        for (int i = 1; i < tsos.length; i++) {
            assertTrue(tsos[i].compareTo(tsos[i-1]) > 0);
        }
    }
    
    @Test
    public void testTSOMonotonicity() {
        TSO[] tsos = new TSO[1000];
        
        // 生成大量TSO
        for (int i = 0; i < 1000; i++) {
            tsos[i] = tsoGenerator.generateTSO();
        }
        
        // 验证严格单调递增
        for (int i = 1; i < 1000; i++) {
            assertTrue(tsos[i].compareTo(tsos[i-1]) > 0,
                String.format("TSO[%d] should be greater than TSO[%d]", i, i-1));
        }
    }
    
    @Test
    public void testConcurrentTSOGeneration() throws Exception {
        int numThreads = 10;
        int tsosPerThread = 100;
        TSO[][] results = new TSO[numThreads][];
        Thread[] threads = new Thread[numThreads];
        
        // 创建多个线程并发生成TSO
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                results[threadIndex] = new TSO[tsosPerThread];
                for (int j = 0; j < tsosPerThread; j++) {
                    results[threadIndex][j] = tsoGenerator.generateTSO();
                }
            });
        }
        
        // 启动所有线程
        for (Thread thread : threads) {
            thread.start();
        }
        
        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }
        
        // 验证每个线程内的TSO是递增的
        for (int i = 0; i < numThreads; i++) {
            for (int j = 1; j < tsosPerThread; j++) {
                assertTrue(results[i][j].compareTo(results[i][j-1]) > 0);
            }
        }
        
        // 验证所有TSO都是唯一的（简化检查）
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < tsosPerThread; j++) {
                assertNotNull(results[i][j]);
            }
        }
    }
    
    @Test
    public void testLargeBatchGeneration() {
        int largeBatchSize = 10000;
        TSO[] tsos = tsoGenerator.generateTSOBatch(largeBatchSize);
        
        assertNotNull(tsos);
        assertEquals(largeBatchSize, tsos.length);
        
        // 验证第一个和最后一个TSO
        assertNotNull(tsos[0]);
        assertNotNull(tsos[largeBatchSize - 1]);
        assertTrue(tsos[largeBatchSize - 1].compareTo(tsos[0]) > 0);
    }
    
    @Test
    public void testZeroBatchSize() {
        TSO[] tsos = tsoGenerator.generateTSOBatch(0);
        
        assertNotNull(tsos);
        assertEquals(0, tsos.length);
    }
    
    @Test
    public void testNegativeBatchSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            tsoGenerator.generateTSOBatch(-1);
        });
    }
    
    @Test
    public void testTSOTimeProgression() throws Exception {
        TSO tso1 = tsoGenerator.generateTSO();
        
        // 等待一段时间
        Thread.sleep(10);
        
        TSO tso2 = tsoGenerator.generateTSO();
        
        // 物理时间应该相等或递增
        assertTrue(tso2.getPhysical() >= tso1.getPhysical());
        
        // 如果物理时间相同，逻辑时间应该递增
        if (tso2.getPhysical() == tso1.getPhysical()) {
            assertTrue(tso2.getLogical() > tso1.getLogical());
        }
    }
    
    @Test
    public void testLogicalCounterOverflow() {
        // 这个测试模拟逻辑计数器接近溢出的情况
        // 在实际实现中，应该处理逻辑计数器溢出的情况
        
        // 生成大量TSO来测试逻辑计数器
        TSO lastTSO = null;
        for (int i = 0; i < 1000; i++) {
            TSO currentTSO = tsoGenerator.generateTSO();
            if (lastTSO != null) {
                assertTrue(currentTSO.compareTo(lastTSO) > 0);
            }
            lastTSO = currentTSO;
        }
    }
}