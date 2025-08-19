package com.milvus.common;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;

import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Etcd 客户端封装
 * 用于元数据存储和服务发现
 */
public class EtcdClient {
    private static final Logger logger = LoggerFactory.getLogger(EtcdClient.class);
    
    private final Client client;
    private final KV kvClient;
    private final Lease leaseClient;
    private final ScheduledExecutorService leaseRenewalExecutor;

    public EtcdClient(String endpoints) {
        this.client = Client.builder()
                .endpoints(endpoints.split(","))
                .user(toByteSequence("root"))
                .password(toByteSequence("milvus@123"))
                .connectTimeout(Duration.ofSeconds(10))
                .keepaliveTime(Duration.ofSeconds(30))
                .keepaliveTimeout(Duration.ofSeconds(20))
                .build();
        this.kvClient = client.getKVClient();
        this.leaseClient = client.getLeaseClient();
        this.leaseRenewalExecutor = Executors.newScheduledThreadPool(1);
        logger.info("Etcd client connected to {}", endpoints);
    }


    public CompletableFuture<Void> putWithLease(String key,String value, long ttlSeconds){
        return leaseClient.grant(ttlSeconds).thenCompose(leaseGrantResponse -> {
            long leaseId = leaseGrantResponse.getID();
            ByteSequence keySeq = toByteSequence(key);
            ByteSequence valueSeq = toByteSequence(value);

            scheduleLeaseRenewal(leaseId,ttlSeconds);

            return kvClient.put(keySeq,valueSeq, PutOption.builder().withLeaseId(leaseId).build()).thenAccept(putResponse -> {
                logger.debug("Put Key with lease:{} = {},leaseId:{}",key,value,leaseId);;
            });
        }).exceptionally(ex -> {
            logger.error("Failed to put key with lease: {}", key, ex);
            return null;
        });
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
        }finally {
            //关闭lease续约线程池
            if (leaseRenewalExecutor!=null && !leaseRenewalExecutor.isShutdown()){
                leaseRenewalExecutor.shutdown();
                try{
                    //最多等5s
                    if (!leaseRenewalExecutor.awaitTermination(5,TimeUnit.SECONDS)){
                        leaseRenewalExecutor.shutdown();
                    }
                }catch (InterruptedException e){
                    leaseRenewalExecutor.shutdownNow();
                    //重新设置当前线程的中断状态
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    public CompletableFuture<List<String>> getAll(String prefix){
        ByteSequence prefixSeq = toByteSequence(prefix);
        GetOption getOption = GetOption.builder()
                .isPrefix(true)
                .build();

        return kvClient.get(prefixSeq, getOption)
                .thenApply(response ->
                        response.getKvs().stream()
                                .map(kv -> kv.getValue().toString(StandardCharsets.UTF_8))
                                .collect(Collectors.toList())
                )
                .whenComplete((values, throwable) -> {
                    if (throwable != null) {
                        logger.error("Failed to get keys with prefix: {}", prefix, throwable);
                    } else {
                        logger.debug("Found {} keys with prefix: {}", values.size(), prefix);
                    }
                });
    }
    public CompletableFuture<List<String>> discoverServiceWithRetry(String serviceName, int maxRetries, long retryIntervalMs) {
        CompletableFuture<List<String>> resultFuture = new CompletableFuture<>();
        discoverServiceWithRetry(serviceName, maxRetries, retryIntervalMs, 0, resultFuture);
        return resultFuture;
    }

    private void discoverServiceWithRetry(String serviceName, int maxRetries, long retryIntervalMs, int attempt, CompletableFuture<List<String>> resultFuture) {
        discoverService(serviceName).whenComplete((services, ex) -> {
            if (ex != null) {
                // 发生异常，检查是否重试
                if (attempt < maxRetries) {
                    logger.debug("Service discovery failed for '{}', retrying ({}/{})...",
                            serviceName, attempt + 1, maxRetries, ex);
                    leaseRenewalExecutor.schedule(() ->
                                    discoverServiceWithRetry(serviceName, maxRetries, retryIntervalMs, attempt + 1, resultFuture),
                            retryIntervalMs, TimeUnit.MILLISECONDS
                    );
                } else {
                    resultFuture.completeExceptionally(ex);
                }
            } else {
                if (!services.isEmpty()) {
                    resultFuture.complete(services);
                } else if (attempt < maxRetries) {
                    // 空结果但还有重试次数
                    logger.debug("No services found for '{}', retrying ({}/{})...",
                            serviceName, attempt + 1, maxRetries);
                    leaseRenewalExecutor.schedule(() ->
                                    discoverServiceWithRetry(serviceName, maxRetries, retryIntervalMs, attempt + 1, resultFuture),
                            retryIntervalMs, TimeUnit.MILLISECONDS
                    );
                } else {
                    // 达到最大重试次数仍为空
                    logger.warn("No services found for '{}' after {} retries",
                            serviceName, maxRetries);
                    resultFuture.complete(Collections.emptyList());
                }
            }
        });
    }

    public CompletableFuture<List<String>> discoverService(String serviceName) {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        String prefix = "/service/"+serviceName+"/";
        ByteSequence key = toByteSequence(prefix);
        GetOption getOption = GetOption.builder()
                .isPrefix(true)
                .build();

        kvClient.get(key, getOption).whenComplete((response, ex) -> {
            if (ex != null) {
                logger.error("Error discovering service: {}", serviceName, ex);
                future.completeExceptionally(ex);
            } else {
                List<String> result = response.getKvs().stream()
                        .map(kv -> {
                            String fullKey = kv.getKey().toString(StandardCharsets.UTF_8);
                            return fullKey.substring(prefix.length());
                        })
                        .collect(Collectors.toList());
                future.complete(result);
            }
        });
        return future;
    }
    public CompletableFuture<Void> registerServiceAsync(String serviceName, String endpoint, int ttlSeconds) {
        String key = "/service/" + serviceName + "/" + endpoint;
        return leaseClient.grant(ttlSeconds)
                .orTimeout(5, TimeUnit.SECONDS)
                .thenCompose(leaseGrant -> {
                    long leaseId = leaseGrant.getID();
                    return kvClient.put(
                            toByteSequence(key),
                            toByteSequence("alive"),
                            PutOption.builder().withLeaseId(leaseId).build()
                    ).thenRun(() -> {
                        logger.info("Registered service: {} at {}", serviceName, endpoint);
                        scheduleLeaseRenewal(leaseId, ttlSeconds);
                    });
                }).exceptionally(ex -> {
                    logger.error("Service registration failed: {}/{}", serviceName, endpoint, ex);
                    return null;
                });
    }

    private ByteSequence toByteSequence(String str){
        return ByteSequence.from(str,StandardCharsets.UTF_8);
    }
    private void scheduleLeaseRenewal(long leaseId,long ttlSeconds){
        long delay = Math.max(1,ttlSeconds-5);
        leaseRenewalExecutor.scheduleAtFixedRate(()->{
            try{
                leaseClient.keepAliveOnce(leaseId).get();
                logger.debug("Lease renewed: {}" ,leaseId);
            }catch (Exception e){
                logger.error("Failed to renew lease:{}",leaseId,e);
            }
        },delay,delay, TimeUnit.SECONDS);
    }
}