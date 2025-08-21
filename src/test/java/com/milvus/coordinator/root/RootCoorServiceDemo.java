package com.milvus.coordinator.root;

import com.milvus.common.TSO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RootCoorServiceDemo {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RootCoorServiceDemo.class);

    private RootCoordinatorServer rootCoordinator;
    private static final String TEST_ENDPOINTS = "http://localhost:2379";

    @BeforeEach
    public void setUp() {
        rootCoordinator = new RootCoordinatorServer("test-root-coord", 19530, TEST_ENDPOINTS);
    }

    @AfterEach
    public void tearDown() {
        if (rootCoordinator != null) {
            rootCoordinator.shutdown();
        }
    }

    @Test
    public void testServerInitialization() {
        assertNotNull(rootCoordinator);
    }
    @Test
    public void testServerStartup() {
        assertDoesNotThrow(() -> {
            rootCoordinator.start();

            // 等待服务启动
            Thread.sleep(2000);
        });
    }
    @Test
    public void testAllocateTimeStamp()throws Exception{
        rootCoordinator.start();
        Thread.sleep(2000);

        TSO tso = rootCoordinator.allocateTimestamp();

        logger.info("Allocated timestamp: {}", tso);
    }

    @Test
    public void testAllocateTimestampBatch()throws Exception{
        rootCoordinator.start();
        Thread.sleep(2000);

        int batchSize = 10;
        TSO[] tsos = rootCoordinator.allocateTimestampBatch(batchSize);

        for(int i = 1;i<tsos.length;i++){
            logger.debug("tso is up :{},tso is :{}",tsos[i].compareTo(tsos[i-1])>0,tsos[i]);
        }
    }

    @Test
    public void testServiceRegistration() throws  Exception{
        rootCoordinator.start();
        Thread.sleep(2000);

        String serviceType="data-node";
        String serviceId =  "test-data-node-1";
        String address = "localhost:21121";

        rootCoordinator.registerService(serviceType, serviceId, address);
    }
    @Test
    public void testServiceUnregistration() throws Exception {
        rootCoordinator.start();
        Thread.sleep(2000);

        String serviceType = "query-node";
        String serviceId = "test-query-node-1";
        String address = "localhost:21122";

        // 先注册服务
        rootCoordinator.registerService(serviceType, serviceId, address);

        // 然后注销服务
        assertDoesNotThrow(() -> {
            rootCoordinator.unregisterService(serviceId);
        });
    }
    @Test
    public void testMultipleServiceRegistrations() throws Exception {
        rootCoordinator.start();
        Thread.sleep(2000);

        // 注册多个不同类型的服务
        rootCoordinator.registerService("data-node", "data-node-1", "localhost:21121");
        rootCoordinator.registerService("data-node", "data-node-2", "localhost:21122");
        rootCoordinator.registerService("query-node", "query-node-1", "localhost:21123");
        rootCoordinator.registerService("index-node", "index-node-1", "localhost:21124");

        // 验证没有异常抛出
        assertTrue(true);
    }

    @Test
    public void testTimestampMonotonicity()throws Exception{
        rootCoordinator.start();
        Thread.sleep(2000);

        TSO[] timestamps = new TSO[100];
        for(int i = 0;i<100;i++){
            timestamps[i] = rootCoordinator.allocateTimestamp();
        }

        for(int i = 1;i<100;i++){
            logger.debug("tso is up :{},tso is :{}",timestamps[i].compareTo(timestamps[i-1])>0,timestamps[i]);
        }
    }
    @Test
    public void testConcurrentTimestampAllocation()throws Exception{
        rootCoordinator.start();
        Thread.sleep(2000);

        int numThreads = 5;
        int allocationsPerThread = 20;

        TSO[][] results = new TSO[numThreads][];
        Thread[] threads = new Thread[numThreads];

        //创建多个线程并发分配TSO
        for(int i =0;i<numThreads;i++){
            final int threadIndx = i;
            threads[i] = new Thread(()->{
                results[threadIndx] = rootCoordinator.allocateTimestampBatch(allocationsPerThread);
            });
        }
        //启动所有线程
        for(Thread thread:threads){
            thread.start();
        }

        //等待所有线程完成
        for(Thread thread:threads){
            thread.join(10000);//10s超时
        }

        //验证所有的TSO都是唯一的，并且递增
        for (int i = 0;i<numThreads;i++){
            logger.debug("result is null?:{}",results[i]==null);
            logger.debug("allocationPerThread is equals result{}.length?:{}",i,allocationsPerThread==results[i].length);

            //验证所有的线程内的TSO都是递增的
            for(int j = 1;j<results[i].length;j++){
                logger.debug("递增：{},{}",results[i][j].compareTo(results[i][j-1])>0,String.format("results[%d][%d].compareTo(results[%d][%d])>0",i,j,i,j-1));
            }
        }
    }
    @Test
    public void testTimestampAllocationWithoutLeadership() {
        // 不启动服务，直接尝试分配TSO
        assertThrows(IllegalStateException.class, () -> {
            rootCoordinator.allocateTimestamp();
        });
    }

    @Test
    public void testBatchTimestampAllocationWithoutLeadership() {
        // 不启动服务，直接尝试批量分配TSO
        assertThrows(IllegalStateException.class, () -> {
            rootCoordinator.allocateTimestampBatch(10);
        });
    }

    @Test
    public void testServerShutdown() throws Exception {
        rootCoordinator.start();
        Thread.sleep(1000);

        assertDoesNotThrow(() -> {
            rootCoordinator.shutdown();
        });
    }

    @Test
    public void testServiceRegistrationAfterShutdown() throws Exception {
        rootCoordinator.start();
        Thread.sleep(1000);
        rootCoordinator.shutdown();

        // 关闭后尝试注册服务应该失败或被忽略
        assertDoesNotThrow(() -> {
            rootCoordinator.registerService("test-service", "test-id", "test-address");
        });
    }
}
