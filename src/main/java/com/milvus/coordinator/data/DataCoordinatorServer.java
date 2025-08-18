package com.milvus.coordinator.data;

import com.milvus.common.TSO;
import com.milvus.common.EtcdClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Data Coordinator 服务器
 * 负责Segment分配、Flush触发、数据节点管理
 */
public class DataCoordinatorServer {
    private static final Logger logger = LoggerFactory.getLogger(DataCoordinatorServer.class);
    
    private final DataNodeLoadBalancer loadBalancer;
    private final EtcdClient etcdClient;
    private final ScheduledExecutorService scheduler;
    
    // Segment管理
    private final Map<String, SegmentInfo> segments = new ConcurrentHashMap<>();
    private final Map<String, CollectionInfo> collections = new ConcurrentHashMap<>();
    
    // 配置参数
    private final String nodeId;
    private final int port;
    private final long segmentMaxSize = 512 * 1024 * 1024; // 512MB
    private final long flushInterval = 60000; // 60秒
    
    public DataCoordinatorServer(String nodeId, int port, String etcdEndpoints) {
        this.nodeId = nodeId;
        this.port = port;
        this.loadBalancer = new DataNodeLoadBalancer(DataNodeLoadBalancer.Strategy.LEAST_LOAD);
        this.etcdClient = new EtcdClient(etcdEndpoints);
        this.scheduler = Executors.newScheduledThreadPool(4);
        
        logger.info("DataCoordinator initialized: nodeId={}, port={}", nodeId, port);
    }
    
    /**
     * 启动服务
     */
    public void start() {
        try {
            // 启动Segment监控
            startSegmentMonitoring();
            
            // 启动Flush调度
            startFlushScheduler();
            
            // 启动节点健康检查
            startNodeHealthCheck();
            
            logger.info("DataCoordinator started successfully");
            
        } catch (Exception e) {
            logger.error("Failed to start DataCoordinator", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 分配Segment
     */
    public String allocateSegment(String collectionId, String partitionId, TSO tso) {
        // 选择数据节点
        String selectedNode = loadBalancer.selectNode(collectionId, partitionId);
        if (selectedNode == null) {
            throw new RuntimeException("No available data node for segment allocation");
        }
        
        // 生成Segment ID
        String segmentId = generateSegmentId(collectionId, partitionId, tso);
        
        // 创建Segment信息
        SegmentInfo segmentInfo = new SegmentInfo(segmentId, collectionId, partitionId, 
                                                 selectedNode, tso, System.currentTimeMillis());
        segments.put(segmentId, segmentInfo);
        
        // 持久化到Etcd
        String key = String.format("/milvus/segments/%s", segmentId);
        etcdClient.put(key, segmentInfo.toJson());
        
        logger.info("Segment allocated: id={}, collection={}, partition={}, node={}", 
                   segmentId, collectionId, partitionId, selectedNode);
        
        return segmentId;
    }
    
    /**
     * 触发Segment Flush
     */
    public void triggerFlush(String segmentId) {
        SegmentInfo segmentInfo = segments.get(segmentId);
        if (segmentInfo == null) {
            logger.warn("Segment not found for flush: {}", segmentId);
            return;
        }
        
        if (segmentInfo.getState() != SegmentState.GROWING) {
            logger.warn("Segment not in growing state: {}, state={}", segmentId, segmentInfo.getState());
            return;
        }
        
        // 更新Segment状态为Flushing
        segmentInfo.setState(SegmentState.FLUSHING);
        segmentInfo.setFlushTime(System.currentTimeMillis());
        
        // 通知数据节点执行Flush
        notifyDataNodeFlush(segmentInfo.getNodeId(), segmentId);
        
        logger.info("Flush triggered for segment: {}", segmentId);
    }
    
    /**
     * 注册数据节点
     */
    public void registerDataNode(String nodeId, String address, int capacity) {
        loadBalancer.registerNode(nodeId, address, capacity);
        
        // 持久化节点信息
        String key = String.format("/milvus/data-nodes/%s", nodeId);
        String value = String.format("{\"address\":\"%s\",\"capacity\":%d,\"registerTime\":%d}",
                                   address, capacity, System.currentTimeMillis());
        etcdClient.put(key, value);
    }
    
    /**
     * 更新节点负载
     */
    public void updateNodeLoad(String nodeId, int currentLoad, int memoryUsage, int cpuUsage) {
        loadBalancer.updateNodeLoad(nodeId, currentLoad, memoryUsage, cpuUsage);
    }
    
    /**
     * 启动Segment监控
     */
    private void startSegmentMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                segments.values().forEach(segment -> {
                    // 检查Segment大小，触发自动Flush
                    if (segment.getState() == SegmentState.GROWING && 
                        segment.getSize() >= segmentMaxSize) {
                        triggerFlush(segment.getSegmentId());
                    }
                });
            } catch (Exception e) {
                logger.error("Segment monitoring failed", e);
            }
        }, 10, 10, TimeUnit.SECONDS);
    }
    
    /**
     * 启动Flush调度器
     */
    private void startFlushScheduler() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                long currentTime = System.currentTimeMillis();
                segments.values().forEach(segment -> {
                    // 定时Flush长时间未Flush的Segment
                    if (segment.getState() == SegmentState.GROWING && 
                        (currentTime - segment.getCreateTime()) >= flushInterval) {
                        triggerFlush(segment.getSegmentId());
                    }
                });
            } catch (Exception e) {
                logger.error("Flush scheduler failed", e);
            }
        }, flushInterval, flushInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 启动节点健康检查
     */
    private void startNodeHealthCheck() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                loadBalancer.getAllNodes().forEach((nodeId, nodeInfo) -> {
                    if (!nodeInfo.isHealthy()) {
                        logger.warn("Unhealthy data node detected: {}", nodeInfo);
                        // 可以在这里实现节点故障转移逻辑
                    }
                });
            } catch (Exception e) {
                logger.error("Node health check failed", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
    
    /**
     * 通知数据节点执行Flush
     */
    private void notifyDataNodeFlush(String nodeId, String segmentId) {
        // 这里应该通过gRPC调用数据节点的Flush接口
        logger.info("Notifying data node {} to flush segment {}", nodeId, segmentId);
    }
    
    /**
     * 生成Segment ID
     */
    private String generateSegmentId(String collectionId, String partitionId, TSO tso) {
        return String.format("%s_%s_%d", collectionId, partitionId, tso.encode());
    }
    
    /**
     * 关闭服务
     */
    public void shutdown() {
        try {
            scheduler.shutdown();
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            
            etcdClient.close();
            logger.info("DataCoordinator shutdown completed");
            
        } catch (Exception e) {
            logger.error("Error during shutdown", e);
        }
    }
    
    /**
     * Segment状态枚举
     */
    public enum SegmentState {
        GROWING,    // 增长中
        FLUSHING,   // 刷盘中
        FLUSHED,    // 已刷盘
        SEALED      // 已封存
    }
    
    /**
     * Segment信息类
     */
    public static class SegmentInfo {
        private final String segmentId;
        private final String collectionId;
        private final String partitionId;
        private final String nodeId;
        private final TSO startTSO;
        private final long createTime;
        private volatile SegmentState state = SegmentState.GROWING;
        private volatile long size = 0;
        private volatile long flushTime = 0;
        
        public SegmentInfo(String segmentId, String collectionId, String partitionId, 
                          String nodeId, TSO startTSO, long createTime) {
            this.segmentId = segmentId;
            this.collectionId = collectionId;
            this.partitionId = partitionId;
            this.nodeId = nodeId;
            this.startTSO = startTSO;
            this.createTime = createTime;
        }
        
        // Getters and Setters
        public String getSegmentId() { return segmentId; }
        public String getCollectionId() { return collectionId; }
        public String getPartitionId() { return partitionId; }
        public String getNodeId() { return nodeId; }
        public TSO getStartTSO() { return startTSO; }
        public long getCreateTime() { return createTime; }
        public SegmentState getState() { return state; }
        public void setState(SegmentState state) { this.state = state; }
        public long getSize() { return size; }
        public void setSize(long size) { this.size = size; }
        public long getFlushTime() { return flushTime; }
        public void setFlushTime(long flushTime) { this.flushTime = flushTime; }
        
        public String toJson() {
            return String.format("{\"segmentId\":\"%s\",\"collectionId\":\"%s\",\"partitionId\":\"%s\"," +
                               "\"nodeId\":\"%s\",\"startTSO\":%d,\"createTime\":%d,\"state\":\"%s\",\"size\":%d}",
                               segmentId, collectionId, partitionId, nodeId, startTSO.encode(), 
                               createTime, state, size);
        }
    }
    
    /**
     * Collection信息类
     */
    public static class CollectionInfo {
        private final String collectionId;
        private final String schema;
        private final long createTime;
        
        public CollectionInfo(String collectionId, String schema, long createTime) {
            this.collectionId = collectionId;
            this.schema = schema;
            this.createTime = createTime;
        }
        
        public String getCollectionId() { return collectionId; }
        public String getSchema() { return schema; }
        public long getCreateTime() { return createTime; }
    }
}