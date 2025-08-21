package com.milvus.common;

import com.milvus.common.EtcdClient;
import io.etcd.jetcd.kv.PutResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 测试服务
 * 节点向etcd中注册，
 * 节点在etcd中的查询
 */
public class EtcdDemo {
    private static final Logger logger = LoggerFactory.getLogger(EtcdClient.class);
    private EtcdClient etcdClient;

    private static final String Test_Endpoints = "http://localhost:2379";

    @BeforeEach
    public void setUp(){
        etcdClient = new EtcdClient(Test_Endpoints);
    }
    @AfterEach
    public void tearDown(){
        if (etcdClient!=null){
            etcdClient.close();
        }
    }

    @Test
    public void testPutAndGet() throws ExecutionException, InterruptedException, TimeoutException {
        String key = "test/key1";
        String value = "test_value_1";

        CompletableFuture<PutResponse> putFuture = etcdClient.put(key,value);
        putFuture.get(5,TimeUnit.SECONDS);

        CompletableFuture<String> getFuture = etcdClient.get(key);
        String retrievedValue = getFuture.get(5,TimeUnit.SECONDS);

        logger.debug("TestPutAndGet的测试结果为：：：value:{} and retrievedValue:{}，是否一致：{}",value,retrievedValue,value.equals(retrievedValue));
    }

    @Test
    public void testGetNotExistentKey() throws ExecutionException, InterruptedException, TimeoutException {
        String key = "test/non_existent_key";
        CompletableFuture<String> getFuture = etcdClient.get(key);
        String result = getFuture.get(5,TimeUnit.SECONDS);
        logger.debug("TestGetNotExistentKey的测试结果为：：：result:{}",result);
    }

    @Test
    public void testGetAll() throws ExecutionException, InterruptedException, TimeoutException {
        String prefix = "test/prefix/";
        etcdClient.put(prefix+"key1","value1").get(5,TimeUnit.SECONDS);
        etcdClient.put(prefix+"key2","value2").get(5,TimeUnit.SECONDS);
        etcdClient.put(prefix+"key3","value3").get(5,TimeUnit.SECONDS);

        CompletableFuture<List<String>> getAllFuture = etcdClient.getAll(prefix);
        List<String> values=  getAllFuture.get(5,TimeUnit.SECONDS);

        logger.debug("TestGetAll的测试结果为：：：values:{}",values);
    }

    @Test
    public void testDelete() throws ExecutionException, InterruptedException, TimeoutException {
        String key = "test/delete_key";
        String value = "delete_value";

        //先插入
        etcdClient.put(key,value).get(5,TimeUnit.SECONDS);

        //验证是否存在
        String retrievedValue = etcdClient.get(key).get(5, TimeUnit.SECONDS);
        if (retrievedValue==null){
            logger.debug("TestDelete的测试结果为：：：key:{}不存在",key);
        }else {
            //删除
            CompletableFuture<Object> deleteFuture = etcdClient.delete(key);
            deleteFuture.get(5,TimeUnit.SECONDS);

            //验证删除
            String deletedValue = etcdClient.get(key).get(5,TimeUnit.SECONDS);
            if (deletedValue==null){
                logger.debug("TestDelete的测试结果为：：：key:{}已删除",key);
            }
        }
    }

    @Test
    public void testConcurrentOperations() throws ExecutionException, InterruptedException, TimeoutException {
        int numOperations = 10;
        CompletableFuture<Void>[] futures = new CompletableFuture[numOperations];

        //并发执行多个put操作
        for(int i = 0;i<numOperations;i++){
            final int index = i;
            CompletableFuture<PutResponse> put = etcdClient.put("test/concurrent/"+index,"value"+index);
            futures[i]  = put.thenApply(putResponse -> null);
        }
        //等待所有操作完成
        CompletableFuture.allOf(futures).get(10,TimeUnit.SECONDS);

        //验证所有的值都已经正确的存储
        for(int i = 0;i<numOperations;i++){
            String value = etcdClient.get("test/concurrent/"+i).get(5,TimeUnit.SECONDS);
            if (value==null||!value.equals("value"+i)){
                logger.error("TestConcurrentOperations的测试结果为：：：key:{}的值不正确",i);
            }
        }
    }


    @Test
    public void RegisterAndDiscoverService() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture.allOf(
                etcdClient.registerServiceAsync("query-node","node1:7000",30),
                etcdClient.registerServiceAsync("query-node","node2:7001",30)
                ).thenCompose(v->{
                    etcdClient.discoverServiceWithRetry("query-node",5,300)
                            .thenAccept(service->{
                                if (service.isEmpty()){
                                    logger.warn("No query nodes found after retries");
                                }else {
                                    logger.info("Query-Nodes:" + service);
                                }
                            }).exceptionally(ex->{
                                logger.error("Failed to discover query nodes:{}",ex.getMessage());
                                return null;
                            });

                    return etcdClient.put("config/max-connections","1000")
                            .thenCompose( __ ->etcdClient.getAll("config/"))
                            .thenAccept(configs->{
                                logger.info("all configs:"+configs);
                                etcdClient.close();
                            });

        }).exceptionally(ex->{
            System.err.println("Operation failed: " + ex.getMessage());
            etcdClient.close();
            return null;
        }).get(20,TimeUnit.SECONDS);
    }

}
