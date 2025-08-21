package com.milvus.common;

import com.milvus.coordinator.root.TSOGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSODemo {
    private final static Logger logger = LoggerFactory.getLogger(TSODemo.class);

    @Test
    public void testTSOCreation() {
        long physical = System.currentTimeMillis();
        long logical = 100L;
        TSO tso = new TSO(physical, logical);
        logger.debug("physical equals tso.physical:{}", physical - tso.getPhysical() == 0);
        logger.debug("logical equals tso.logical:{}", logical - tso.getLogical() == 0);
    }

    @Test
    public void testTsoEncoding() {
        long physical = 1640995200000L; // 2022-01-01 00:00:00
        long logical = 12345L;

        TSO tso = new TSO(physical, logical);
        long encode = tso.encode();

        logger.debug("tso.encode:{}", encode);
        TSO decode = TSO.decode(encode);

        logger.debug("tso.decode:{}", decode);

        logger.debug("physical equals tso.physical:{}", physical - tso.getPhysical() == 0);
        logger.debug("logical equals tso.logical:{}", logical - tso.getLogical() == 0);
    }

    @Test
    public void testTsoComparsion() {
        TSO tso1 = new TSO(1000L, 100L);
        TSO tso2 = new TSO(1000L, 200L);
        TSO tso3 = new TSO(2000L, 50L);

        logger.debug("tso1.compareTo(tso2):{}", tso1.compareTo(tso2) < 0);
        logger.debug("tso2.compareTo(tso3):{}", tso2.compareTo(tso1) > 0);
        logger.debug("tso3.compareTo(tso1):{}", tso1.compareTo(tso3) < 0);
        logger.debug("tso1.compareTo(tso1):{}", tso3.compareTo(tso1) > 0);
    }

    @Test
    public void testTSOLogicalOverflow() {
        TSO tso = new TSO(1000L, Long.MAX_VALUE);
        long encoded = tso.encode();
        TSO decoded = TSO.decode(encoded);
        logger.debug("tso.decode:{}", decoded);
    }

    @Test
    public void testTSOtoString() {
        TSO tso = new TSO(1000L, 100L);
        String str = tso.toString();
        logger.debug("tso.toString:{}", str);
    }

    //    ----------------------------------------------
//    下面的部分验证TSO的生成逻辑
    private TSOGenerator tsoGenerator;

    @BeforeEach
    public void setUp() {
        tsoGenerator = new TSOGenerator();
    }

    @Test
    public void testGenerateSingleTSO() {
        TSO tso = tsoGenerator.generateTSO();

        logger.debug("tso:{}", tso);
    }

    @Test
    public void testGenerateMultipleTSO() {
        TSO[] tsos = tsoGenerator.generateTSOBatch(10);
        for (int i = 1; i < tsos.length; i++) {
            logger.debug("tsos[{}]:{}", i, tsos[i]);
        }
    }

    @Test
    public void testTSOMonotonicity() {
        TSO[] tsos = new TSO[100];

        for (int i = 0; i < 1000; i++) {
            tsos[i] = tsoGenerator.generateTSO();
        }

        // 验证生成TSO的单调递增
        for (int i = 1; i < 100; i++) {
            logger.debug("递增：{},{}", tsos[i].compareTo(tsos[i - 1]) > 0, String.format("tsos[%d].compareTo(tsos[%d])>0", i, i - 1));
        }

    }

    @Test
    public void testConcurrentTSOGeneration() throws Exception {
        TSO[][] results = new TSO[10][];
        Thread[] threads = new Thread[10];

        //创建多个线程并发生成TSO 每个线程负责生成一组TSO
        for (int i = 0; i < 10; i++) {
            final int threadIndex = i;
            //创建一个线程
            threads[i] = new Thread(() -> {
                //为当前的线程分配一个TSO数组，用于存储该线程生成的所有的TSO
                results[threadIndex] = new TSO[50];
                //在当前线程中生成100个TSO，并将它们放入线程的TSO数组中
                for (int j = 0; j < 50; j++) {
                    //生成TSO，放入数组
                    results[threadIndex][j] = tsoGenerator.generateTSO();
                }
            });
        }

        //启动所有线程
        for (Thread thread : threads) {
            thread.start();
        }
        //等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }
        //验证每个线程内的TSO是递增的
        for (int i = 0; i < 10; i++) {
            for (int j = 1; j < 50; j++) {
                logger.debug("递增：{},{}", results[i][j].compareTo(results[i][j - 1]) > 0, String.format("results[%d][%d].compareTo(results[%d][%d])>0", i, j, i, j - 1));
            }
        }
        //验证所有TSO都是唯一的
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 49; j++) {
                logger.debug("唯一：{},{}", results[i][j].equals(results[i][j + 1]), String.format("results[%d][%d].equals(results[%d][%d])", i, j, i, j + 1));
            }
        }

    }

    @Test
    public void testLargeBatchGeneration() {
        int largeBatchSize = 100;
        TSO[] tsos =tsoGenerator.generateTSOBatch(largeBatchSize);
        logger.debug("Generated {} TSOs", tsos.length);
        logger.debug("TSO first:{},last:{}", tsos[0], tsos[tsos.length - 1]);
    }

    @Test
    public void testTSOTimeProgression() throws InterruptedException {
        TSO tso1 = tsoGenerator.generateTSO();
        Thread.sleep(10);

        TSO tso2 = tsoGenerator.generateTSO();

        logger.debug("tso2>tso1:{}",tso2.getPhysical()>tso1.getPhysical());

        if (tso2.getPhysical() == tso1.getPhysical()){
            logger.debug("tso2.getLogical()>tso1.getLogical():{}",tso2.getLogical()>tso1.getLogical());
        }
    }

}
