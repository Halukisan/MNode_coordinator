package com.milvus.coordinator.root;

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
 * Root Coordinator 服务器
 * 负责TSO生成、元数据管理、集群协调
 */
public class RootCoordinatorServer {
    private static final Logger logger = LoggerFactory.getLogger(RootCoordinatorServer.class);
    
    private final TSOGenerator tsoGenerator;
    private final EtcdClient etcdClient;
    private final ScheduledExecutorService scheduler;
    
    // 服务注册信息
    private final Map<String, ServiceInfo> registeredServices = new ConcurrentHashMap<>();
    
    // 配置参数
    private final String nodeId;
    private final int port;
    private volatile boolean isLeader = false;
    
    public RootCoordinatorServer(String nodeId, int port, String etcdEndpoints) {
        this.nodeId = nodeId;
        this.port = port;
        this.tsoGenerator = new TSOGenerator();
        this.etcdClient = new EtcdClient(etcdEndpoints);
        this.scheduler = Executors.newScheduledThreadPool(4);
        
        logger.info("RootCoordinator initialized: nodeId={}, port={}", nodeId, port);
    }
    
    /**
     * 启动服务
     */
    public void start() {
        try {
            // 尝试成为Leader
            electLeader();
            
            // 启动心跳检测
            startHeartbeat();
            
            // 启动服务发现
            startServiceDiscovery();
            
            // 启动元数据清理
            startMetadataCleanup();
            
            logger.info("RootCoordinator started successfully");
            
        } catch (Exception e) {
            logger.error("Failed to start RootCoordinator", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 生成TSO
     */
    public TSO allocateTimestamp() {
        if (!isLeader) {
            throw new IllegalStateException("Only leader can allocate timestamp");
        }
        return tsoGenerator.generateTSO();
    }
    
    /**
     * 批量生成TSO
     */
    public TSO[] allocateTimestampBatch(int count) {
        if (!isLeader) {
            throw new IllegalStateException("Only leader can allocate timestamp");
        }
        return tsoGenerator.generateTSOBatch(count);
    }
    
    /**
     * 注册服务
     */
    public void registerService(String serviceType, String serviceId, String address) {
        ServiceInfo serviceInfo = new ServiceInfo(serviceType, serviceId, address, System.currentTimeMillis());
        registeredServices.put(serviceId, serviceInfo);
        
        // 持久化到Etcd
        String key = String.format("/milvus/services/%s/%s", serviceType, serviceId);
        etcdClient.put(key, serviceInfo.toJson());
        
        logger.info("Service registered: type={}, id={}, address={}", serviceType, serviceId, address);
    }
    
    /**
     * 注销服务
     */
    public void unregisterService(String serviceId) {
        ServiceInfo serviceInfo = registeredServices.remove(serviceId);
        if (serviceInfo != null) {
            String key = String.format("/milvus/services/%s/%s", serviceInfo.getType(), serviceId);
            etcdClient.delete(key);
            logger.info("Service unregistered: id={}", serviceId);
        }
    }
    
    /**
     * Leader选举
     */
    private void electLeader() {
        // 简化的Leader选举逻辑
        String leaderKey = "/milvus/leader/root-coordinator";
        String leaderValue = nodeId + ":" + port;
        
        etcdClient.get(leaderKey)
                .thenCompose(currentLeader -> {
                    if (currentLeader == null) {
                        // 没有Leader，尝试成为Leader
                        return etcdClient.put(leaderKey, leaderValue)
                                .thenApply(response -> {
                                    isLeader = true;
                                    logger.info("Became leader: {}", nodeId);
                                    return true;
                                });
                    } else {
                        logger.info("Current leader: {}", currentLeader);
                        return java.util.concurrent.CompletableFuture.completedFuture(false);
                    }
                })
                .exceptionally(throwable -> {
                    logger.error("Leader election failed", throwable);
                    return false;
                });
    }
    
    /**
     * 启动心跳检测
     */
    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (isLeader) {
                    String heartbeatKey = "/milvus/heartbeat/root-coordinator/" + nodeId;
                    etcdClient.put(heartbeatKey, String.valueOf(System.currentTimeMillis()));
                }
            } catch (Exception e) {
                logger.error("Heartbeat failed", e);
            }
        }, 0, 10, TimeUnit.SECONDS);
    }
    
    /**
     * 启动服务发现
     */
    private void startServiceDiscovery() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // 检查注册服务的健康状态
                registeredServices.entrySet().removeIf(entry -> {
                    ServiceInfo service = entry.getValue();
                    long lastSeen = System.currentTimeMillis() - service.getLastHeartbeat();
                    if (lastSeen > 60000) { // 60秒超时
                        logger.warn("Service timeout: {}", entry.getKey());
                        return true;
                    }
                    return false;
                });
            } catch (Exception e) {
                logger.error("Service discovery failed", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
    
    /**
     * 启动元数据清理
     */
    private void startMetadataCleanup() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // 清理过期的元数据
                logger.debug("Metadata cleanup executed");
            } catch (Exception e) {
                logger.error("Metadata cleanup failed", e);
            }
        }, 0, 300, TimeUnit.SECONDS); // 5分钟执行一次
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
            logger.info("RootCoordinator shutdown completed");
            
        } catch (Exception e) {
            logger.error("Error during shutdown", e);
        }
    }
    
    /**
     * 服务信息内部类
     */
    private static class ServiceInfo {
        private final String type;
        private final String id;
        private final String address;
        private final long lastHeartbeat;
        
        public ServiceInfo(String type, String id, String address, long lastHeartbeat) {
            this.type = type;
            this.id = id;
            this.address = address;
            this.lastHeartbeat = lastHeartbeat;
        }
        
        public String getType() { return type; }
        public String getId() { return id; }
        public String getAddress() { return address; }
        public long getLastHeartbeat() { return lastHeartbeat; }
        
        public String toJson() {
            return String.format("{\"type\":\"%s\",\"id\":\"%s\",\"address\":\"%s\",\"lastHeartbeat\":%d}",
                               type, id, address, lastHeartbeat);
        }
    }
}