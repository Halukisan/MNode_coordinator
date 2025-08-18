package com.milvus.storage;

import com.milvus.common.EtcdClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Etcd 管理器
 * 提供高级的元数据管理功能
 */
public class EtcdManager {
    private static final Logger logger = LoggerFactory.getLogger(EtcdManager.class);
    
    private final EtcdClient etcdClient;
    private final ObjectMapper objectMapper;
    
    // 元数据缓存
    private final Map<String, Object> metadataCache = new ConcurrentHashMap<>();
    
    // 键前缀常量
    private static final String COLLECTION_PREFIX = "/milvus/collections/";
    private static final String SEGMENT_PREFIX = "/milvus/segments/";
    private static final String NODE_PREFIX = "/milvus/nodes/";
    private static final String SCHEMA_PREFIX = "/milvus/schemas/";
    private static final String INDEX_PREFIX = "/milvus/indexes/";
    
    public EtcdManager(String endpoints) {
        this.etcdClient = new EtcdClient(endpoints);
        this.objectMapper = new ObjectMapper();
        
        logger.info("EtcdManager initialized with endpoints: {}", endpoints);
    }
    
    /**
     * 存储Collection元数据
     */
    public CompletableFuture<Void> storeCollectionMetadata(String collectionId, CollectionMetadata metadata) {
        String key = COLLECTION_PREFIX + collectionId;
        
        try {
            String value = objectMapper.writeValueAsString(metadata);
            return etcdClient.put(key, value)
                    .thenApply(response -> {
                        metadataCache.put(key, metadata);
                        logger.info("Collection metadata stored: {}", collectionId);
                        return null;
                    });
        } catch (Exception e) {
            logger.error("Failed to serialize collection metadata: {}", collectionId, e);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    /**
     * 获取Collection元数据
     */
    public CompletableFuture<CollectionMetadata> getCollectionMetadata(String collectionId) {
        String key = COLLECTION_PREFIX + collectionId;
        
        // 先检查缓存
        CollectionMetadata cached = (CollectionMetadata) metadataCache.get(key);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }
        
        return etcdClient.get(key)
                .thenApply(value -> {
                    if (value == null) {
                        return null;
                    }
                    
                    try {
                        CollectionMetadata metadata = objectMapper.readValue(value, CollectionMetadata.class);
                        metadataCache.put(key, metadata);
                        return metadata;
                    } catch (Exception e) {
                        logger.error("Failed to deserialize collection metadata: {}", collectionId, e);
                        throw new RuntimeException(e);
                    }
                });
    }
    
    /**
     * 存储Segment元数据
     */
    public CompletableFuture<Void> storeSegmentMetadata(String segmentId, SegmentMetadata metadata) {
        String key = SEGMENT_PREFIX + segmentId;
        
        try {
            String value = objectMapper.writeValueAsString(metadata);
            return etcdClient.put(key, value)
                    .thenApply(response -> {
                        metadataCache.put(key, metadata);
                        logger.debug("Segment metadata stored: {}", segmentId);
                        return null;
                    });
        } catch (Exception e) {
            logger.error("Failed to serialize segment metadata: {}", segmentId, e);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    /**
     * 获取Segment元数据
     */
    public CompletableFuture<SegmentMetadata> getSegmentMetadata(String segmentId) {
        String key = SEGMENT_PREFIX + segmentId;
        
        SegmentMetadata cached = (SegmentMetadata) metadataCache.get(key);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }
        
        return etcdClient.get(key)
                .thenApply(value -> {
                    if (value == null) {
                        return null;
                    }
                    
                    try {
                        SegmentMetadata metadata = objectMapper.readValue(value, SegmentMetadata.class);
                        metadataCache.put(key, metadata);
                        return metadata;
                    } catch (Exception e) {
                        logger.error("Failed to deserialize segment metadata: {}", segmentId, e);
                        throw new RuntimeException(e);
                    }
                });
    }
    
    /**
     * 注册节点
     */
    public CompletableFuture<Void> registerNode(String nodeType, String nodeId, NodeMetadata metadata) {
        String key = NODE_PREFIX + nodeType + "/" + nodeId;
        
        try {
            String value = objectMapper.writeValueAsString(metadata);
            return etcdClient.put(key, value)
                    .thenApply(response -> {
                        logger.info("Node registered: type={}, id={}", nodeType, nodeId);
                        return null;
                    });
        } catch (Exception e) {
            logger.error("Failed to register node: type={}, id={}", nodeType, nodeId, e);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    /**
     * 注销节点
     */
    public CompletableFuture<Void> unregisterNode(String nodeType, String nodeId) {
        String key = NODE_PREFIX + nodeType + "/" + nodeId;
        
        return etcdClient.delete(key)
                .thenApply(response -> {
                    logger.info("Node unregistered: type={}, id={}", nodeType, nodeId);
                    return null;
                });
    }
    
    /**
     * 存储Schema
     */
    public CompletableFuture<Void> storeSchema(String collectionId, String schema) {
        String key = SCHEMA_PREFIX + collectionId;
        
        return etcdClient.put(key, schema)
                .thenApply(response -> {
                    logger.info("Schema stored for collection: {}", collectionId);
                    return null;
                });
    }
    
    /**
     * 获取Schema
     */
    public CompletableFuture<String> getSchema(String collectionId) {
        String key = SCHEMA_PREFIX + collectionId;
        return etcdClient.get(key);
    }
    
    /**
     * 存储索引元数据
     */
    public CompletableFuture<Void> storeIndexMetadata(String indexId, IndexMetadata metadata) {
        String key = INDEX_PREFIX + indexId;
        
        try {
            String value = objectMapper.writeValueAsString(metadata);
            return etcdClient.put(key, value)
                    .thenApply(response -> {
                        logger.info("Index metadata stored: {}", indexId);
                        return null;
                    });
        } catch (Exception e) {
            logger.error("Failed to store index metadata: {}", indexId, e);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    /**
     * 清理过期元数据
     */
    public void cleanupExpiredMetadata() {
        long currentTime = System.currentTimeMillis();
        long expireTime = 24 * 60 * 60 * 1000; // 24小时
        
        metadataCache.entrySet().removeIf(entry -> {
            // 这里可以根据具体的元数据类型实现过期逻辑
            return false; // 简化实现
        });
        
        logger.debug("Metadata cache cleanup completed");
    }
    
    /**
     * 获取缓存统计信息
     */
    public String getCacheStats() {
        return String.format("MetadataCache{size=%d}", metadataCache.size());
    }
    
    /**
     * 关闭管理器
     */
    public void close() {
        try {
            metadataCache.clear();
            etcdClient.close();
            logger.info("EtcdManager closed");
        } catch (Exception e) {
            logger.error("Error closing EtcdManager", e);
        }
    }
    
    /**
     * Collection元数据类
     */
    public static class CollectionMetadata {
        private String collectionId;
        private String name;
        private String schema;
        private long createTime;
        private int partitionCount;
        private String state;
        
        // 构造函数、getter和setter
        public CollectionMetadata() {}
        
        public CollectionMetadata(String collectionId, String name, String schema, 
                                long createTime, int partitionCount, String state) {
            this.collectionId = collectionId;
            this.name = name;
            this.schema = schema;
            this.createTime = createTime;
            this.partitionCount = partitionCount;
            this.state = state;
        }
        
        // Getters and Setters
        public String getCollectionId() { return collectionId; }
        public void setCollectionId(String collectionId) { this.collectionId = collectionId; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getSchema() { return schema; }
        public void setSchema(String schema) { this.schema = schema; }
        public long getCreateTime() { return createTime; }
        public void setCreateTime(long createTime) { this.createTime = createTime; }
        public int getPartitionCount() { return partitionCount; }
        public void setPartitionCount(int partitionCount) { this.partitionCount = partitionCount; }
        public String getState() { return state; }
        public void setState(String state) { this.state = state; }
    }
    
    /**
     * Segment元数据类
     */
    public static class SegmentMetadata {
        private String segmentId;
        private String collectionId;
        private String partitionId;
        private String nodeId;
        private long startTSO;
        private long endTSO;
        private String state;
        private long size;
        private long rowCount;
        private String storageLocation;
        
        public SegmentMetadata() {}
        
        // Getters and Setters
        public String getSegmentId() { return segmentId; }
        public void setSegmentId(String segmentId) { this.segmentId = segmentId; }
        public String getCollectionId() { return collectionId; }
        public void setCollectionId(String collectionId) { this.collectionId = collectionId; }
        public String getPartitionId() { return partitionId; }
        public void setPartitionId(String partitionId) { this.partitionId = partitionId; }
        public String getNodeId() { return nodeId; }
        public void setNodeId(String nodeId) { this.nodeId = nodeId; }
        public long getStartTSO() { return startTSO; }
        public void setStartTSO(long startTSO) { this.startTSO = startTSO; }
        public long getEndTSO() { return endTSO; }
        public void setEndTSO(long endTSO) { this.endTSO = endTSO; }
        public String getState() { return state; }
        public void setState(String state) { this.state = state; }
        public long getSize() { return size; }
        public void setSize(long size) { this.size = size; }
        public long getRowCount() { return rowCount; }
        public void setRowCount(long rowCount) { this.rowCount = rowCount; }
        public String getStorageLocation() { return storageLocation; }
        public void setStorageLocation(String storageLocation) { this.storageLocation = storageLocation; }
    }
    
    /**
     * 节点元数据类
     */
    public static class NodeMetadata {
        private String nodeId;
        private String nodeType;
        private String address;
        private int port;
        private long registerTime;
        private String state;
        private Map<String, Object> properties;
        
        public NodeMetadata() {}
        
        public NodeMetadata(String nodeId, String nodeType, String address, int port) {
            this.nodeId = nodeId;
            this.nodeType = nodeType;
            this.address = address;
            this.port = port;
            this.registerTime = System.currentTimeMillis();
            this.state = "ACTIVE";
            this.properties = new ConcurrentHashMap<>();
        }
        
        // Getters and Setters
        public String getNodeId() { return nodeId; }
        public void setNodeId(String nodeId) { this.nodeId = nodeId; }
        public String getNodeType() { return nodeType; }
        public void setNodeType(String nodeType) { this.nodeType = nodeType; }
        public String getAddress() { return address; }
        public void setAddress(String address) { this.address = address; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public long getRegisterTime() { return registerTime; }
        public void setRegisterTime(long registerTime) { this.registerTime = registerTime; }
        public String getState() { return state; }
        public void setState(String state) { this.state = state; }
        public Map<String, Object> getProperties() { return properties; }
        public void setProperties(Map<String, Object> properties) { this.properties = properties; }
    }
    
    /**
     * 索引元数据类
     */
    public static class IndexMetadata {
        private String indexId;
        private String collectionId;
        private String fieldName;
        private String indexType;
        private Map<String, Object> parameters;
        private String state;
        private long createTime;
        
        public IndexMetadata() {}
        
        // Getters and Setters
        public String getIndexId() { return indexId; }
        public void setIndexId(String indexId) { this.indexId = indexId; }
        public String getCollectionId() { return collectionId; }
        public void setCollectionId(String collectionId) { this.collectionId = collectionId; }
        public String getFieldName() { return fieldName; }
        public void setFieldName(String fieldName) { this.fieldName = fieldName; }
        public String getIndexType() { return indexType; }
        public void setIndexType(String indexType) { this.indexType = indexType; }
        public Map<String, Object> getParameters() { return parameters; }
        public void setParameters(Map<String, Object> parameters) { this.parameters = parameters; }
        public String getState() { return state; }
        public void setState(String state) { this.state = state; }
        public long getCreateTime() { return createTime; }
        public void setCreateTime(long createTime) { this.createTime = createTime; }
    }
}