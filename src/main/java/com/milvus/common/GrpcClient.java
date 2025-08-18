package com.milvus.common;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * gRPC 通信工具类
 * 提供连接管理和通用的gRPC客户端功能
 */
public class GrpcClient {
    private static final Logger logger = LoggerFactory.getLogger(GrpcClient.class);
    
    private final ManagedChannel channel;
    private final String target;
    
    public GrpcClient(String host, int port) {
        this.target = host + ":" + port;
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .maxInboundMessageSize(4 * 1024 * 1024) // 4MB
                .build();
        
        logger.info("gRPC client connected to {}", target);
    }
    
    public ManagedChannel getChannel() {
        return channel;
    }
    
    /**
     * 创建gRPC stub
     */
    public <T extends AbstractStub<T>> T createStub(Class<T> stubClass) {
        try {
            return stubClass.getConstructor(ManagedChannel.class).newInstance(channel);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create gRPC stub for " + stubClass.getName(), e);
        }
    }
    
    /**
     * 关闭连接
     */
    public void shutdown() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            logger.info("gRPC client to {} shutdown completed", target);
        } catch (InterruptedException e) {
            logger.warn("gRPC client shutdown interrupted", e);
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 检查连接状态
     */
    public boolean isConnected() {
        return !channel.isShutdown() && !channel.isTerminated();
    }
}