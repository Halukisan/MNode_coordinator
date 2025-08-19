package com.milvus.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;

/**
 * EtcdManager 测试用例
 * 注意：需要启动etcd服务才能运行这些测试
 */
public class EtcdManagerTest {
    
    private EtcdManager etcdManager;
    private static final String TEST_ENDPOINTS = "http://localhost:2379";
    
    @BeforeEach
    public void setUp() {
        etcdManager = new EtcdManager(TEST_ENDPOINTS);
    }
    
    @AfterEach
    public void tearDown() {
        if (etcdManager != null) {
            etcdManager.close();
        }
    }
    
    @Test
    public void testStoreAndGetCollectionMetadata() throws Exception {
        String collectionId = "test-collection-1";
        EtcdManager.CollectionMetadata metadata = new EtcdManager.CollectionMetadata(
            collectionId, "test_collection", "test_schema", 
            System.currentTimeMillis(), 4, "ACTIVE"
        );
        
        // 存储元数据
        etcdManager.storeCollectionMetadata(collectionId, metadata).get(5, TimeUnit.SECONDS);
        
        // 获取元数据
        EtcdManager.CollectionMetadata retrieved = etcdManager.getCollectionMetadata(collectionId)
            .get(5, TimeUnit.SECONDS);
        
        assertNotNull(retrieved);
        assertEquals(collectionId, retrieved.getCollectionId());
        assertEquals("test_collection", retrieved.getName());
        assertEquals("test_schema", retrieved.getSchema());
        assertEquals(4, retrieved.getPartitionCount());
        assertEquals("ACTIVE", retrieved.getState());
    }
    
    @Test
    public void testGetNonExistentCollectionMetadata() throws Exception {
        String collectionId = "non-existent-collection";
        
        EtcdManager.CollectionMetadata metadata = etcdManager.getCollectionMetadata(collectionId)
            .get(5, TimeUnit.SECONDS);
        
        assertNull(metadata);
    }
    
    @Test
    public void testStoreAndGetSegmentMetadata() throws Exception {
        String segmentId = "test-segment-1";
        EtcdManager.SegmentMetadata metadata = new EtcdManager.SegmentMetadata();
        metadata.setSegmentId(segmentId);
        metadata.setCollectionId("test-collection");
        metadata.setPartitionId("test-partition");
        metadata.setNodeId("data-node-1");
        metadata.setStartTSO(1000L);
        metadata.setEndTSO(2000L);
        metadata.setState("GROWING");
        metadata.setSize(1024L);
        metadata.setRowCount(100L);
        metadata.setStorageLocation("s3://bucket/path");
        
        // 存储Segment元数据
        etcdManager.storeSegmentMetadata(segmentId, metadata).get(5, TimeUnit.SECONDS);
        
        // 获取Segment元数据
        EtcdManager.SegmentMetadata retrieved = etcdManager.getSegmentMetadata(segmentId)
            .get(5, TimeUnit.SECONDS);
        
        assertNotNull(retrieved);
        assertEquals(segmentId, retrieved.getSegmentId());
        assertEquals("test-collection", retrieved.getCollectionId());
        assertEquals("test-partition", retrieved.getPartitionId());
        assertEquals("data-node-1", retrieved.getNodeId());
        assertEquals(1000L, retrieved.getStartTSO());
        assertEquals(2000L, retrieved.getEndTSO());
        assertEquals("GROWING", retrieved.getState());
        assertEquals(1024L, retrieved.getSize());
        assertEquals(100L, retrieved.getRowCount());
        assertEquals("s3://bucket/path", retrieved.getStorageLocation());
    }
    
    @Test
    public void testRegisterAndUnregisterNode() throws Exception {
        String nodeType = "data-node";
        String nodeId = "test-node-1";
        EtcdManager.NodeMetadata metadata = new EtcdManager.NodeMetadata(
            nodeId, nodeType, "localhost", 8080
        );
        
        // 注册节点
        etcdManager.registerNode(nodeType, nodeId, metadata).get(5, TimeUnit.SECONDS);
        
        // 注销节点
        etcdManager.unregisterNode(nodeType, nodeId).get(5, TimeUnit.SECONDS);
        
        // 验证操作成功（这里主要测试没有异常抛出）
        assertTrue(true);
    }
    
    @Test
    public void testStoreAndGetSchema() throws Exception {
        String collectionId = "test-collection-schema";
        String schema = "{\"fields\":[{\"name\":\"id\",\"type\":\"int64\"},{\"name\":\"vector\",\"type\":\"float_vector\",\"dim\":128}]}";
        
        // 存储Schema
        etcdManager.storeSchema(collectionId, schema).get(5, TimeUnit.SECONDS);
        
        // 获取Schema
        String retrievedSchema = etcdManager.getSchema(collectionId).get(5, TimeUnit.SECONDS);
        
        assertEquals(schema, retrievedSchema);
    }
    
    @Test
    public void testStoreIndexMetadata() throws Exception {
        String indexId = "test-index-1";
        EtcdManager.IndexMetadata metadata = new EtcdManager.IndexMetadata();
        metadata.setIndexId(indexId);
        metadata.setCollectionId("test-collection");
        metadata.setFieldName("vector");
        metadata.setIndexType("IVF_FLAT");
        metadata.setState("BUILDING");
        metadata.setCreateTime(System.currentTimeMillis());
        
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("nlist", 1024);
        parameters.put("metric_type", "L2");
        metadata.setParameters(parameters);
        
        // 存储索引元数据
        etcdManager.storeIndexMetadata(indexId, metadata).get(5, TimeUnit.SECONDS);
        
        // 验证操作成功
        assertTrue(true);
    }
    
    @Test
    public void testNodeMetadataProperties() {
        String nodeId = "test-node";
        String nodeType = "query-node";
        String address = "192.168.1.100";
        int port = 9090;
        
        EtcdManager.NodeMetadata metadata = new EtcdManager.NodeMetadata(nodeId, nodeType, address, port);
        
        assertEquals(nodeId, metadata.getNodeId());
        assertEquals(nodeType, metadata.getNodeType());
        assertEquals(address, metadata.getAddress());
        assertEquals(port, metadata.getPort());
        assertEquals("ACTIVE", metadata.getState());
        assertNotNull(metadata.getProperties());
        assertTrue(metadata.getRegisterTime() > 0);
        
        // 测试属性设置
        metadata.getProperties().put("cpu_cores", 8);
        metadata.getProperties().put("memory_gb", 32);
        
        assertEquals(8, metadata.getProperties().get("cpu_cores"));
        assertEquals(32, metadata.getProperties().get("memory_gb"));
    }
    
    @Test
    public void testCacheStats() {
        String stats = etcdManager.getCacheStats();
        assertNotNull(stats);
        assertTrue(stats.contains("MetadataCache"));
    }
    
    @Test
    public void testCleanupExpiredMetadata() {
        // 测试清理方法不抛出异常
        assertDoesNotThrow(() -> {
            etcdManager.cleanupExpiredMetadata();
        });
    }
    
    @Test
    public void testConcurrentMetadataOperations() throws Exception {
        int numCollections = 10;
        
        // 并发存储多个Collection元数据
        for (int i = 0; i < numCollections; i++) {
            String collectionId = "concurrent-collection-" + i;
            EtcdManager.CollectionMetadata metadata = new EtcdManager.CollectionMetadata(
                collectionId, "collection_" + i, "schema_" + i,
                System.currentTimeMillis(), 2, "ACTIVE"
            );
            
            etcdManager.storeCollectionMetadata(collectionId, metadata);
        }
        
        // 等待所有操作完成
        Thread.sleep(2000);
        
        // 验证所有元数据都能正确获取
        for (int i = 0; i < numCollections; i++) {
            String collectionId = "concurrent-collection-" + i;
            EtcdManager.CollectionMetadata retrieved = etcdManager.getCollectionMetadata(collectionId)
                .get(5, TimeUnit.SECONDS);
            
            assertNotNull(retrieved);
            assertEquals(collectionId, retrieved.getCollectionId());
            assertEquals("collection_" + i, retrieved.getName());
        }
    }
}