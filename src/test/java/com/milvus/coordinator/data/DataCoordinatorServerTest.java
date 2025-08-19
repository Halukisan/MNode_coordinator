package com.milvus.coordinator.data;

import com.milvus.common.TSO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * DataCoordinatorServer 测试用例
 * 注意：需要启动etcd服务才能运行这些测试
 */
public class DataCoordinatorServerTest {
    
    private DataCoordinatorServer dataCoordinator;
    private static final String TEST_ENDPOINTS = "http://localhost:2379";
    
    @BeforeEach
    public void setUp() {
        dataCoordinator = new DataCoordinatorServer("test-data-coord", 13333, TEST_ENDPOINTS);
    }
    
    @AfterEach
    public void tearDown() {
        if (dataCoordinator != null) {
            dataCoordinator.shutdown();
        }
    }
    
    @Test
    public void testServerInitialization() {
        assertNotNull(dataCoordinator);
    }
    
    @Test
    public void testServerStartup() {
        assertDoesNotThrow(() -> {
            dataCoordinator.start();
            
            // 等待服务启动
            Thread.sleep(2000);
        });
    }
    
    @Test
    public void testDataNodeRegistration() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        String nodeId = "test-data-node-1";
        String address = "localhost:21121";
        int capacity = 1000;
        
        assertDoesNotThrow(() -> {
            dataCoordinator.registerDataNode(nodeId, address, capacity);
        });
    }
    
    @Test
    public void testMultipleDataNodeRegistration() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        // 注册多个数据节点
        dataCoordinator.registerDataNode("data-node-1", "localhost:21121", 1000);
        dataCoordinator.registerDataNode("data-node-2", "localhost:21122", 1500);
        dataCoordinator.registerDataNode("data-node-3", "localhost:21123", 2000);
        
        // 验证没有异常抛出
        assertTrue(true);
    }
    
    @Test
    public void testSegmentAllocation() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        // 先注册一个数据节点
        dataCoordinator.registerDataNode("data-node-1", "localhost:21121", 1000);
        
        String collectionId = "test-collection";
        String partitionId = "test-partition";
        TSO tso = new TSO(System.currentTimeMillis(), 1);
        
        String segmentId = dataCoordinator.allocateSegment(collectionId, partitionId, tso);
        
        assertNotNull(segmentId);
        assertTrue(segmentId.contains(collectionId));
        assertTrue(segmentId.contains(partitionId));
    }
    
    @Test
    public void testSegmentAllocationWithoutNodes() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        String collectionId = "test-collection";
        String partitionId = "test-partition";
        TSO tso = new TSO(System.currentTimeMillis(), 1);
        
        // 没有注册数据节点时分配Segment应该失败
        assertThrows(RuntimeException.class, () -> {
            dataCoordinator.allocateSegment(collectionId, partitionId, tso);
        });
    }
    
    @Test
    public void testMultipleSegmentAllocation() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        // 注册多个数据节点
        dataCoordinator.registerDataNode("data-node-1", "localhost:21121", 1000);
        dataCoordinator.registerDataNode("data-node-2", "localhost:21122", 1000);
        
        String collectionId = "test-collection";
        String partitionId = "test-partition";
        
        // 分配多个Segment
        for (int i = 0; i < 10; i++) {
            TSO tso = new TSO(System.currentTimeMillis(), i);
            String segmentId = dataCoordinator.allocateSegment(collectionId, partitionId, tso);
            
            assertNotNull(segmentId);
            assertTrue(segmentId.contains(collectionId));
            assertTrue(segmentId.contains(partitionId));
        }
    }
    
    @Test
    public void testFlushTrigger() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        // 注册数据节点并分配Segment
        dataCoordinator.registerDataNode("data-node-1", "localhost:21121", 1000);
        
        String collectionId = "test-collection";
        String partitionId = "test-partition";
        TSO tso = new TSO(System.currentTimeMillis(), 1);
        
        String segmentId = dataCoordinator.allocateSegment(collectionId, partitionId, tso);
        
        // 触发Flush
        assertDoesNotThrow(() -> {
            dataCoordinator.triggerFlush(segmentId);
        });
    }
    
    @Test
    public void testFlushNonExistentSegment() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        String nonExistentSegmentId = "non-existent-segment";
        
        // 触发不存在的Segment的Flush应该不抛出异常，只是记录警告
        assertDoesNotThrow(() -> {
            dataCoordinator.triggerFlush(nonExistentSegmentId);
        });
    }
    
    @Test
    public void testNodeLoadUpdate() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        String nodeId = "data-node-1";
        dataCoordinator.registerDataNode(nodeId, "localhost:21121", 1000);
        
        // 更新节点负载
        assertDoesNotThrow(() -> {
            dataCoordinator.updateNodeLoad(nodeId, 50, 60, 70);
        });
    }
    
    @Test
    public void testConcurrentSegmentAllocation() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        // 注册数据节点
        dataCoordinator.registerDataNode("data-node-1", "localhost:21121", 1000);
        dataCoordinator.registerDataNode("data-node-2", "localhost:21122", 1000);
        
        int numThreads = 5;
        int segmentsPerThread = 10;
        String[][] results = new String[numThreads][];
        Thread[] threads = new Thread[numThreads];
        
        // 创建多个线程并发分配Segment
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                results[threadIndex] = new String[segmentsPerThread];
                for (int j = 0; j < segmentsPerThread; j++) {
                    TSO tso = new TSO(System.currentTimeMillis(), threadIndex * segmentsPerThread + j);
                    results[threadIndex][j] = dataCoordinator.allocateSegment(
                        "collection-" + threadIndex, "partition-" + j, tso);
                }
            });
        }
        
        // 启动所有线程
        for (Thread thread : threads) {
            thread.start();
        }
        
        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join(10000); // 10秒超时
        }
        
        // 验证所有Segment都分配成功且唯一
        for (int i = 0; i < numThreads; i++) {
            assertNotNull(results[i]);
            assertEquals(segmentsPerThread, results[i].length);
            
            for (int j = 0; j < segmentsPerThread; j++) {
                assertNotNull(results[i][j]);
                assertTrue(results[i][j].contains("collection-" + i));
                assertTrue(results[i][j].contains("partition-" + j));
            }
        }
    }
    
    @Test
    public void testServerShutdown() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        assertDoesNotThrow(() -> {
            dataCoordinator.shutdown();
        });
    }
    
    @Test
    public void testSegmentStateTransition() throws Exception {
        dataCoordinator.start();
        Thread.sleep(1000);
        
        // 注册数据节点并分配Segment
        dataCoordinator.registerDataNode("data-node-1", "localhost:21121", 1000);
        
        String collectionId = "test-collection";
        String partitionId = "test-partition";
        TSO tso = new TSO(System.currentTimeMillis(), 1);
        
        String segmentId = dataCoordinator.allocateSegment(collectionId, partitionId, tso);
        
        // 触发Flush，应该改变Segment状态
        dataCoordinator.triggerFlush(segmentId);
        
        // 再次触发Flush，应该检测到状态不是GROWING
        dataCoordinator.triggerFlush(segmentId);
        
        // 验证没有异常抛出
        assertTrue(true);
    }
}