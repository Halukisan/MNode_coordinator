package com.milvus.common;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.kv.PutResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * Etcd 客户端封装
 * 用于元数据存储和服务发现
 */
public class EtcdClient {
    private static final Logger logger = LoggerFactory.getLogger(EtcdClient.class);
    
    private final Client client;
    private final KV kvClient;
    
    public EtcdClient(String endpoints) {
        this.client = Client.builder()
                .endpoints(endpoints.split(","))
                .build();
        this.kvClient = client.getKVClient();
        
        logger.info("Etcd client connected to {}", endpoints);
    }
    
    /**
     * 存储键值对
     */
    public CompletableFuture<PutResponse> put(String key, String value) {
        ByteSequence keySeq = ByteSequence.from(key, StandardCharsets.UTF_8);
        ByteSequence valueSeq = ByteSequence.from(value, StandardCharsets.UTF_8);
        
        return kvClient.put(keySeq, valueSeq)
                .whenComplete((response, throwable) -> {
                    if (throwable != null) {
                        logger.error("Failed to put key: {}", key, throwable);
                    } else {
                        logger.debug("Put key: {} = {}", key, value);
                    }
                });
    }
    
    /**
     * 获取值
     */
    public CompletableFuture<String> get(String key) {
        ByteSequence keySeq = ByteSequence.from(key, StandardCharsets.UTF_8);
        
        return kvClient.get(keySeq)
                .thenApply(response -> {
                    if (response.getKvs().isEmpty()) {
                        return null;
                    }
                    return response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
                })
                .whenComplete((value, throwable) -> {
                    if (throwable != null) {
                        logger.error("Failed to get key: {}", key, throwable);
                    } else {
                        logger.debug("Get key: {} = {}", key, value);
                    }
                });
    }
    
    /**
     * 删除键
     */
    public CompletableFuture<Object> delete(String key) {
        ByteSequence keySeq = ByteSequence.from(key, StandardCharsets.UTF_8);
        
        return kvClient.delete(keySeq)
                .thenApply(response -> null)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        logger.error("Failed to delete key: {}", key, throwable);
                    } else {
                        logger.debug("Deleted key: {}", key);
                    }
                });
    }
    
    /**
     * 关闭客户端
     */
    public void close() {
        try {
            kvClient.close();
            client.close();
            logger.info("Etcd client closed");
        } catch (Exception e) {
            logger.error("Error closing Etcd client", e);
        }
    }
}