package com.milvus;

import com.milvus.coordinator.root.RootCoordinatorServer;
import com.milvus.coordinator.data.DataCoordinatorServer;
import com.milvus.node.data.DataNodeServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Milvus-like 系统启动类
 * 演示如何启动各个组件
 */
public class MilvusApplication {
    private static final Logger logger = LoggerFactory.getLogger(MilvusApplication.class);
    
    public static void main(String[] args) {
        logger.info("Starting Milvus-like system...");
        
        // 配置参数
        String etcdEndpoints = "http://localhost:2379";
        String kafkaServers = "localhost:9092";
        
        try {
            // 启动Root Coordinator
            RootCoordinatorServer rootCoordinator = new RootCoordinatorServer(
                "root-coord-1", 19530, etcdEndpoints);
            rootCoordinator.start();
            
            // 启动Data Coordinator
            DataCoordinatorServer dataCoordinator = new DataCoordinatorServer(
                "data-coord-1", 13333, etcdEndpoints);
            dataCoordinator.start();
            
            // 启动Data Node
            DataNodeServer dataNode = new DataNodeServer(
                "data-node-1", "localhost", 21121, kafkaServers, etcdEndpoints);
            dataNode.start();
            
            logger.info("All components started successfully");
            
            // 注册关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down Milvus-like system...");
                
                try {
                    dataNode.shutdown();
                    dataCoordinator.shutdown();
                    rootCoordinator.shutdown();
                    
                    logger.info("System shutdown completed");
                } catch (Exception e) {
                    logger.error("Error during shutdown", e);
                }
            }));
            
            // 保持主线程运行
            Thread.currentThread().join();
            
        } catch (Exception e) {
            logger.error("Failed to start system", e);
            System.exit(1);
        }
    }
}