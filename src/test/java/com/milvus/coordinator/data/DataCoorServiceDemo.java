package com.milvus.coordinator.data;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

public class DataCoorServiceDemo {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(DataCoorServiceDemo.class);

    private DataCoordinatorServer dataCoordinator;

    private static final String TEST_ENDPOINTS = "http://localhost:2379";

    @BeforeEach
    public void setUp() {
       dataCoordinator = new DataCoordinatorServer("test-data-coord",13333,TEST_ENDPOINTS);
    }
    @AfterEach
    public void tearDown(){
        if (dataCoordinator!=null){
            dataCoordinator.shutdown();
        }
    }

    @Test
    public void testServiceStartUP() throws InterruptedException {
        dataCoordinator.start();
        Thread.sleep(2000);
    }
}
