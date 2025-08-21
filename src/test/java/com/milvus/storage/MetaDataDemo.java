package com.milvus.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 存储元数据和状态信息，本质上和etcd的操作一样，只是做了一个封装
 */
public class MetaDataDemo {
    private final static Logger logger = LoggerFactory.getLogger(MetaDataDemo.class);

    private EtcdManager etcdManager;
    private static final String TEST_ENDPOINTS = "http://localhost:2379";
    @BeforeEach
    public void setUp(){
        etcdManager = new EtcdManager(TEST_ENDPOINTS);
    }
    @AfterEach
    public void tearDown(){
        if (etcdManager!=null){
            etcdManager.close();
        }
    }

    @Test
    public void testStoreAndGetCollectionMetaData() throws ExecutionException, InterruptedException, TimeoutException {
        String collectionId = "test-collection-1";
        EtcdManager.CollectionMetadata metadata = new EtcdManager.CollectionMetadata(
                collectionId,"test_collection","test_schema",System.currentTimeMillis(),4,"ACTIVE"
        );

        //存储元数据
        etcdManager.storeCollectionMetadata(collectionId,metadata).get(5, TimeUnit.SECONDS);

        //获取元数据
        EtcdManager.CollectionMetadata retrieved = etcdManager.getCollectionMetadata(collectionId).get(5, TimeUnit.SECONDS);

        if (collectionId.equals(retrieved.getCollectionId())){
            logger.info("~(￣▽￣)~*Retrieved collection metadata: collectionID :{} , name :{}",retrieved.getCollectionId(),retrieved.getName());
        }
    }

    @Test
    public void testStoreAndGetSegmentMetaData() throws Exception{
        String segmentId = "test-segment-1";
        EtcdManager.SegmentMetadata metadata = new EtcdManager.SegmentMetadata();
        metadata.setSegmentId(segmentId);
        metadata.setCollectionId("test-collection");
        metadata.setPartitionId("test-partition");
        metadata.setNodeId("data-node-1");
        metadata.setStartTSO(1000L);
        metadata.setEndTSO(2000L);
        metadata.setState("GROWING");
        metadata.setSize(1024L);
        metadata.setRowCount(100L);
        metadata.setStorageLocation("s3://bucket/path");

        etcdManager.storeSegmentMetadata(segmentId,metadata).get(5,TimeUnit.SECONDS);

        //获取segment数据
        EtcdManager.SegmentMetadata retrieved = etcdManager.getSegmentMetadata(segmentId).get(5,TimeUnit.SECONDS);

        if (segmentId.equals(retrieved.getSegmentId())){
            logger.info("Retrieved segment metadata: {}",retrieved.getSegmentId());
            logger.info("Retrieved segment metadata: {}",retrieved.getCollectionId());
            logger.info("Retrieved segment metadata: {}",retrieved.getPartitionId());
            logger.info("Retrieved segment metadata: {}",retrieved.getNodeId());
            logger.info("Retrieved segment metadata: {}",retrieved.getStartTSO());
            logger.info("Retrieved segment metadata: {}",retrieved.getEndTSO());
            logger.info("Retrieved segment metadata: {}",retrieved.getState());
            logger.info("Retrieved segment metadata: {}",retrieved.getSize());
            logger.info("Retrieved segment metadata: {}",retrieved.getRowCount());
            logger.info("Retrieved segment metadata: {}",retrieved.getStorageLocation());
        }
    }

    @Test
    public void testRegisterAndUnregisterNode()throws Exception{
        String nodeType = "data-node";
        String nodeId = "test-node=1";

        EtcdManager.NodeMetadata metadata = new EtcdManager.NodeMetadata(nodeId,nodeType,"localhost",8080);

        //注册节点
        etcdManager.registerNode(nodeType,nodeId,metadata).get(5,TimeUnit.SECONDS);

        //注销节点
        etcdManager.unregisterNode(nodeType,nodeId).get(5,TimeUnit.SECONDS);
    }

    @Test
    public void testStoreAndGetSchema() throws  Exception{
        String collectionId = "test-collection-schema";
        String schema = "{\"fields\":[{\"name\":\"id\",\"type\":\"int32\"},{\"name\":\"vector\",\"type\":\"float_vector\",\"params\":{\"dim\":128}}]}";

        //存储schema
        etcdManager.storeSchema(collectionId,schema).get(5,TimeUnit.SECONDS);

        //获取schema
        String retrievedSchema = etcdManager.getSchema(collectionId).get(5, TimeUnit.SECONDS);

        if (schema.equals(retrievedSchema)){
            logger.info("~(￣▽￣)~*Retrieved schema: {}",retrievedSchema);
        }
    }

    @Test
    public void testStoreIndexMetaData() throws Exception{
        String indexId = "test-index-1";
        EtcdManager.IndexMetadata metadata = new EtcdManager.IndexMetadata();
        metadata.setIndexId(indexId);
        metadata.setCollectionId("test-collection");
        metadata.setFieldName("vector");
        metadata.setIndexType("IVF_FLAT");
        metadata.setState("BUILDING");
        metadata.setCreateTime(System.currentTimeMillis());

        Map<String,Object> parameters = new HashMap<>();
        parameters.put("nlist",1024);
        parameters.put("metric_type","L2");
        metadata.setParameters(parameters);

        //存储索引元数据
        etcdManager.storeIndexMetadata(indexId,metadata).get(5,TimeUnit.SECONDS);

        //获取索引元数据
        String retrieved = etcdManager.getIndexMetadata(indexId).get(5,TimeUnit.SECONDS);

        logger.debug("~(￣▽￣)~*："+retrieved);
    }

    @Test
    public void testCacheStatus(){
        String status = etcdManager.getCacheStats();
        logger.info("Cache status: {}",status);


    }
    @Test
    public void testConcurrentMetadataOperations()throws Exception{
        int numCollections = 10;
        //并发存储多个Collection元数据
        for(int i = 0;i<numCollections;i++){
            String collectionId = "concurrent-collection-"+i;
            EtcdManager.CollectionMetadata metadata = new EtcdManager.CollectionMetadata(
                    collectionId,"collection_"+i,"schema_"+i,System.currentTimeMillis(),2,"ACTIVE"
            );

            etcdManager.storeCollectionMetadata(collectionId,metadata);
        }

        //等待所有的操作完成
        Thread.sleep(2000);

        //验证缓存状态
        logger.debug("~(￣▽￣)~*--->cache Status:{}",etcdManager.getCacheStats());

        //验证获取元数据
        for (int i = 0;i<numCollections;i++){
            String collectionId = "concurrent-collection-"+i;
            EtcdManager.CollectionMetadata retrieved = etcdManager.getCollectionMetadata(collectionId).get(5,TimeUnit.SECONDS);

            if (collectionId.equals(retrieved.getCollectionId())){
                logger.info("{}:Retrieved collection:{} metadata: {}",i,retrieved.getCollectionId(),retrieved.getName());
            }
        }

    }
}
