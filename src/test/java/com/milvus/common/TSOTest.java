package com.milvus.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * TSO 测试用例
 */
public class TSOTest {
    
    @Test
    public void testTSOCreation() {
        long physical = System.currentTimeMillis();
        long logical = 100L;
        
        TSO tso = new TSO(physical, logical);
        
        assertEquals(physical, tso.getPhysical());
        assertEquals(logical, tso.getLogical());
    }
    
    @Test
    public void testTSOEncoding() {
        long physical = 1640995200000L; // 2022-01-01 00:00:00
        long logical = 12345L;
        
        TSO tso = new TSO(physical, logical);
        long encoded = tso.encode();
        
        TSO decoded = TSO.decode(encoded);
        
        assertEquals(physical, decoded.getPhysical());
        assertEquals(logical, decoded.getLogical());
    }
    
    @Test
    public void testTSOComparison() {
        TSO tso1 = new TSO(1000L, 100L);
        TSO tso2 = new TSO(1000L, 200L);
        TSO tso3 = new TSO(2000L, 50L);
        
        assertTrue(tso1.compareTo(tso2) < 0);
        assertTrue(tso2.compareTo(tso1) > 0);
        assertTrue(tso1.compareTo(tso3) < 0);
        assertTrue(tso3.compareTo(tso1) > 0);
    }
    
    @Test
    public void testTSOLogicalOverflow() {
        long physical = 1000L;
        long logical = 0x3FFFFF; // 最大逻辑值
        
        TSO tso = new TSO(physical, logical);
        long encoded = tso.encode();
        TSO decoded = TSO.decode(encoded);
        
        assertEquals(physical, decoded.getPhysical());
        assertEquals(logical, decoded.getLogical());
    }
    
    @Test
    public void testTSOToString() {
        TSO tso = new TSO(1000L, 100L);
        String str = tso.toString();
        
        assertTrue(str.contains("physical=1000"));
        assertTrue(str.contains("logical=100"));
        assertTrue(str.contains("encoded="));
    }
}