package com.milvus.node.data;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataNodeServerDemo {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeServerDemo.class);

    private DataNodeServer dataNode;

    private static final String KAFKA_SERVICE = "localhost:29092";
    private static final String ETCD_ENDPOINTS = "http://localhost:2379";

    @BeforeEach
    public void setUp(){
        dataNode = new DataNodeServer(
                "test-data-node-1",
                "localhost",
                21121,
                KAFKA_SERVICE,
                ETCD_ENDPOINTS
        );
    }

    @AfterEach
    public void tearDown(){
        if (dataNode!=null){
            dataNode.shutdown();
        }
    }

    @Test
    public void testServiceStartup() throws InterruptedException {
        dataNode.start();
        Thread.sleep(2000);
    }
}
