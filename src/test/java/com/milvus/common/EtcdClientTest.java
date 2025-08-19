package com.milvus.common;

import io.etcd.jetcd.kv.PutResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.List;

/**
 * EtcdClient 测试用例
 * 注意：需要启动etcd服务才能运行这些测试
 */
public class EtcdClientTest {
    
    private EtcdClient etcdClient;
    private static final String TEST_ENDPOINTS = "http://localhost:2379";
    
    @BeforeEach
    public void setUp() {
        etcdClient = new EtcdClient(TEST_ENDPOINTS);
    }
    
    @AfterEach
    public void tearDown() {
        if (etcdClient != null) {
            etcdClient.close();
        }
    }
    
    @Test
    public void testPutAndGet() throws Exception {
        String key = "test/key1";
        String value = "test_value_1";
        
        // 测试put操作
        CompletableFuture<PutResponse> putFuture = etcdClient.put(key, value);
        putFuture.get(5, TimeUnit.SECONDS);
        
        // 测试get操作
        CompletableFuture<String> getFuture = etcdClient.get(key);
        String retrievedValue = getFuture.get(5, TimeUnit.SECONDS);
        
        assertEquals(value, retrievedValue);
    }
    
    @Test
    public void testGetNonExistentKey() throws Exception {
        String key = "test/non_existent_key";
        
        CompletableFuture<String> getFuture = etcdClient.get(key);
        String result = getFuture.get(5, TimeUnit.SECONDS);
        
        assertNull(result);
    }
    
    @Test
    public void testPutWithLease() throws Exception {
        String key = "test/lease_key";
        String value = "lease_value";
        long ttl = 10; // 10秒
        
        CompletableFuture<Void> putFuture = etcdClient.putWithLease(key, value, ttl);
        putFuture.get(5, TimeUnit.SECONDS);
        
        // 立即检查值是否存在
        CompletableFuture<String> getFuture = etcdClient.get(key);
        String retrievedValue = getFuture.get(5, TimeUnit.SECONDS);
        
        assertEquals(value, retrievedValue);
    }
    
    @Test
    public void testGetAll() throws Exception {
        String prefix = "test/prefix/";
        
        // 插入多个键值对
        etcdClient.put(prefix + "key1", "value1").get(5, TimeUnit.SECONDS);
        etcdClient.put(prefix + "key2", "value2").get(5, TimeUnit.SECONDS);
        etcdClient.put(prefix + "key3", "value3").get(5, TimeUnit.SECONDS);
        
        // 获取所有以prefix开头的值
        CompletableFuture<List<String>> getAllFuture = etcdClient.getAll(prefix);
        List<String> values = getAllFuture.get(5, TimeUnit.SECONDS);
        
        assertEquals(3, values.size());
        assertTrue(values.contains("value1"));
        assertTrue(values.contains("value2"));
        assertTrue(values.contains("value3"));
    }
    

    

    
    @Test
    public void testDelete() throws Exception {
        String key = "test/delete_key";
        String value = "delete_value";
        
        // 先插入
        etcdClient.put(key, value).get(5, TimeUnit.SECONDS);
        
        // 验证存在
        String retrievedValue = etcdClient.get(key).get(5, TimeUnit.SECONDS);
        assertEquals(value, retrievedValue);
        
        // 删除
        etcdClient.delete(key).get(5, TimeUnit.SECONDS);
        
        // 验证已删除
        String deletedValue = etcdClient.get(key).get(5, TimeUnit.SECONDS);
        assertNull(deletedValue);
    }
    
    @Test
    public void testConcurrentOperations() throws Exception {
        int numOperations = 10;
        CompletableFuture<Void>[] futures = new CompletableFuture[numOperations];
        
        // 并发执行多个put操作
        for (int i = 0; i < numOperations; i++) {
            final int index = i;
            CompletableFuture<PutResponse> put = etcdClient.put("test/concurrent/" + index, "value" + index);
            futures[i] = put.thenApply(response -> null);
        }
        
        // 等待所有操作完成
        CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);
        
        // 验证所有值都已正确存储
        for (int i = 0; i < numOperations; i++) {
            String value = etcdClient.get("test/concurrent/" + i).get(5, TimeUnit.SECONDS);
            assertEquals("value" + i, value);
        }
    }
}