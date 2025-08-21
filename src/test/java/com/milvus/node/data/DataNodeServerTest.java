package com.milvus.node.data;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * DataNodeServer 测试用例
 * 注意：需要启动Kafka和etcd服务才能运行这些测试
 */
public class DataNodeServerTest {
    
    private DataNodeServer dataNode;
    private static final String KAFKA_SERVERS = "localhost:29092";
    private static final String ETCD_ENDPOINTS = "http://localhost:2379";
    
    @BeforeEach
    public void setUp() {
        dataNode = new DataNodeServer(
            "test-data-node-1", 
            "localhost", 
            21121, 
            KAFKA_SERVERS, 
            ETCD_ENDPOINTS
        );
    }
    
    @AfterEach
    public void tearDown() {
        if (dataNode != null) {
            dataNode.shutdown();
        }
    }
    
    @Test
    public void testServerInitialization() {
        assertNotNull(dataNode);
    }
    
    @Test
    public void testServerStartup() {
        assertDoesNotThrow(() -> {
            dataNode.start();
            
            // 等待服务启动
            Thread.sleep(3000);
        });
    }
    
    @Test
    public void testNodeStatusAfterStartup() throws Exception {
        dataNode.start();
        Thread.sleep(2000);
        
        String status = dataNode.getNodeStatus();
        
        assertNotNull(status);
        assertTrue(status.contains("DataNode"));
        assertTrue(status.contains("test-data-node-1"));
    }
    
    @Test
    public void testFlushTrigger() throws Exception {
        dataNode.start();
        Thread.sleep(2000);
        
        String segmentId = "test-segment-1";
        
        // 触发Flush操作
        assertDoesNotThrow(() -> {
            dataNode.triggerFlush(segmentId);
        });
    }
    
    @Test
    public void testFlushNonExistentSegment() throws Exception {
        dataNode.start();
        Thread.sleep(2000);
        
        String nonExistentSegmentId = "non-existent-segment";
        
        // 触发不存在的Segment的Flush应该记录警告但不抛出异常
        assertDoesNotThrow(() -> {
            dataNode.triggerFlush(nonExistentSegmentId);
        });
    }
    
    @Test
    public void testMultipleFlushOperations() throws Exception {
        dataNode.start();
        Thread.sleep(2000);
        
        // 触发多个Segment的Flush
        for (int i = 0; i < 5; i++) {
            String segmentId = "test-segment-" + i;
            dataNode.triggerFlush(segmentId);
        }
        
        // 等待Flush操作完成
        Thread.sleep(1000);
        
        // 验证没有异常抛出
        assertTrue(true);
    }
    
    @Test
    public void testConcurrentFlushOperations() throws Exception {
        dataNode.start();
        Thread.sleep(2000);
        
        int numThreads = 3;
        int flushesPerThread = 5;
        Thread[] threads = new Thread[numThreads];
        
        // 创建多个线程并发触发Flush
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < flushesPerThread; j++) {
                    String segmentId = "concurrent-segment-" + threadIndex + "-" + j;
                    dataNode.triggerFlush(segmentId);
                }
            });
        }
        
        // 启动所有线程
        for (Thread thread : threads) {
            thread.start();
        }
        
        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join(5000); // 5秒超时
        }
        
        // 等待所有Flush操作完成
        Thread.sleep(2000);
        
        // 验证没有异常抛出
        assertTrue(true);
    }
    
    @Test
    public void testServerShutdown() throws Exception {
        dataNode.start();
        Thread.sleep(2000);
        
        assertDoesNotThrow(() -> {
            dataNode.shutdown();
        });
    }
    
    @Test
    public void testShutdownWithoutStartup() {
        // 测试未启动就关闭的情况
        assertDoesNotThrow(() -> {
            dataNode.shutdown();
        });
    }
    
    @Test
    public void testNodeStatusBeforeStartup() {
        String status = dataNode.getNodeStatus();
        
        assertNotNull(status);
        assertTrue(status.contains("DataNode"));
        assertTrue(status.contains("test-data-node-1"));
        assertTrue(status.contains("segments=0"));
    }
    
    @Test
    public void testMultipleStartupCalls() throws Exception {
        // 测试多次调用start方法
        dataNode.start();
        Thread.sleep(1000);
        
        // 再次调用start应该不会出错
        assertDoesNotThrow(() -> {
            dataNode.start();
        });
        
        Thread.sleep(1000);
    }
    
    @Test
    public void testFlushAfterShutdown() throws Exception {
        dataNode.start();
        Thread.sleep(1000);
        dataNode.shutdown();
        
        // 关闭后触发Flush应该不会崩溃
        assertDoesNotThrow(() -> {
            dataNode.triggerFlush("test-segment");
        });
    }
    
    @Test
    public void testNodeConfiguration() {
        // 测试节点配置参数
        DataNodeServer customNode = new DataNodeServer(
            "custom-node", 
            "192.168.1.100", 
            9999, 
            "localhost:9092", 
            "http://localhost:2379"
        );
        
        assertNotNull(customNode);
        
        String status = customNode.getNodeStatus();
        assertTrue(status.contains("custom-node"));
        
        customNode.shutdown();
    }
    
    @Test
    public void testLongRunningOperation() throws Exception {
        dataNode.start();
        Thread.sleep(2000);
        
        // 模拟长时间运行
        for (int i = 0; i < 10; i++) {
            String segmentId = "long-running-segment-" + i;
            dataNode.triggerFlush(segmentId);
            
            // 检查节点状态
            String status = dataNode.getNodeStatus();
            assertNotNull(status);
            
            Thread.sleep(500);
        }
        
        // 验证节点仍然正常工作
        String finalStatus = dataNode.getNodeStatus();
        assertNotNull(finalStatus);
        assertTrue(finalStatus.contains("DataNode"));
    }
}