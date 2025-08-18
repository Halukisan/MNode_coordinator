package com.milvus.coordinator.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DataNode 负载均衡器
 * 实现多种负载均衡算法，为数据写入选择最优节点
 */
public class DataNodeLoadBalancer {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeLoadBalancer.class);
    
    // 负载均衡策略枚举
    public enum Strategy {
        ROUND_ROBIN,    // 轮询
        LEAST_LOAD,     // 最少负载
        CONSISTENT_HASH // 一致性哈希
    }
    
    private final Strategy strategy;
    private final Map<String, DataNodeInfo> dataNodes = new ConcurrentHashMap<>();
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    
    public DataNodeLoadBalancer(Strategy strategy) {
        this.strategy = strategy;
        logger.info("DataNodeLoadBalancer initialized with strategy: {}", strategy);
    }
    
    /**
     * 注册数据节点
     */
    public void registerNode(String nodeId, String address, int capacity) {
        DataNodeInfo nodeInfo = new DataNodeInfo(nodeId, address, capacity);
        dataNodes.put(nodeId, nodeInfo);
        logger.info("DataNode registered: id={}, address={}, capacity={}", nodeId, address, capacity);
    }
    
    /**
     * 注销数据节点
     */
    public void unregisterNode(String nodeId) {
        DataNodeInfo removed = dataNodes.remove(nodeId);
        if (removed != null) {
            logger.info("DataNode unregistered: id={}", nodeId);
        }
    }
    
    /**
     * 选择数据节点
     */
    public String selectNode(String collectionId, String partitionId) {
        if (dataNodes.isEmpty()) {
            throw new IllegalStateException("No available data nodes");
        }
        
        switch (strategy) {
            case ROUND_ROBIN:
                return selectByRoundRobin();
            case LEAST_LOAD:
                return selectByLeastLoad();
            case CONSISTENT_HASH:
                return selectByConsistentHash(collectionId + ":" + partitionId);
            default:
                throw new IllegalArgumentException("Unknown strategy: " + strategy);
        }
    }
    
    /**
     * 轮询选择
     */
    private String selectByRoundRobin() {
        List<String> nodeIds = new ArrayList<>(dataNodes.keySet());
        if (nodeIds.isEmpty()) {
            return null;
        }
        
        int index = roundRobinIndex.getAndIncrement() % nodeIds.size();
        String selectedNode = nodeIds.get(index);
        
        logger.debug("Selected node by round-robin: {}", selectedNode);
        return selectedNode;
    }
    
    /**
     * 最少负载选择
     */
    private String selectByLeastLoad() {
        return dataNodes.entrySet().stream()
                .min(Comparator.comparingDouble(entry -> entry.getValue().getLoadRatio()))
                .map(Map.Entry::getKey)
                .orElse(null);
    }
    
    /**
     * 一致性哈希选择
     */
    private String selectByConsistentHash(String key) {
        if (dataNodes.isEmpty()) {
            return null;
        }
        
        // 简化的一致性哈希实现
        int hash = key.hashCode();
        List<String> sortedNodes = new ArrayList<>(dataNodes.keySet());
        sortedNodes.sort(String::compareTo);
        
        int index = Math.abs(hash) % sortedNodes.size();
        String selectedNode = sortedNodes.get(index);
        
        logger.debug("Selected node by consistent hash: key={}, node={}", key, selectedNode);
        return selectedNode;
    }
    
    /**
     * 更新节点负载信息
     */
    public void updateNodeLoad(String nodeId, int currentLoad, int memoryUsage, int cpuUsage) {
        DataNodeInfo nodeInfo = dataNodes.get(nodeId);
        if (nodeInfo != null) {
            nodeInfo.updateLoad(currentLoad, memoryUsage, cpuUsage);
            logger.debug("Updated node load: id={}, load={}, memory={}%, cpu={}%", 
                        nodeId, currentLoad, memoryUsage, cpuUsage);
        }
    }
    
    /**
     * 获取所有节点状态
     */
    public Map<String, DataNodeInfo> getAllNodes() {
        return new HashMap<>(dataNodes);
    }
    
    /**
     * 获取可用节点数量
     */
    public int getAvailableNodeCount() {
        return (int) dataNodes.values().stream()
                .filter(DataNodeInfo::isHealthy)
                .count();
    }
    
    /**
     * 数据节点信息类
     */
    public static class DataNodeInfo {
        private final String nodeId;
        private final String address;
        private final int capacity;
        private volatile int currentLoad = 0;
        private volatile int memoryUsage = 0;
        private volatile int cpuUsage = 0;
        private volatile long lastUpdateTime = System.currentTimeMillis();
        
        public DataNodeInfo(String nodeId, String address, int capacity) {
            this.nodeId = nodeId;
            this.address = address;
            this.capacity = capacity;
        }
        
        public void updateLoad(int currentLoad, int memoryUsage, int cpuUsage) {
            this.currentLoad = currentLoad;
            this.memoryUsage = memoryUsage;
            this.cpuUsage = cpuUsage;
            this.lastUpdateTime = System.currentTimeMillis();
        }
        
        public double getLoadRatio() {
            return capacity > 0 ? (double) currentLoad / capacity : 1.0;
        }
        
        public boolean isHealthy() {
            // 节点健康检查：负载不超过90%，内存使用不超过85%，CPU使用不超过80%
            return getLoadRatio() < 0.9 && memoryUsage < 85 && cpuUsage < 80 
                   && (System.currentTimeMillis() - lastUpdateTime) < 60000; // 1分钟内有更新
        }
        
        // Getters
        public String getNodeId() { return nodeId; }
        public String getAddress() { return address; }
        public int getCapacity() { return capacity; }
        public int getCurrentLoad() { return currentLoad; }
        public int getMemoryUsage() { return memoryUsage; }
        public int getCpuUsage() { return cpuUsage; }
        public long getLastUpdateTime() { return lastUpdateTime; }
        
        @Override
        public String toString() {
            return String.format("DataNodeInfo{id='%s', address='%s', load=%d/%d(%.1f%%), memory=%d%%, cpu=%d%%, healthy=%s}",
                               nodeId, address, currentLoad, capacity, getLoadRatio() * 100, 
                               memoryUsage, cpuUsage, isHealthy());
        }
    }
}