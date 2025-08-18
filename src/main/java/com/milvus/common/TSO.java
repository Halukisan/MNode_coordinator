package com.milvus.common;

/**
 * TSO (Timestamp Oracle) 实体
 * 包含物理时间戳和逻辑计数器，保证全局唯一性和单调递增
 */
public class TSO {
    private final long physical;  // 物理时间戳 (毫秒)
    private final long logical;   // 逻辑计数器
    
    public TSO(long physical, long logical) {
        this.physical = physical;
        this.logical = logical;
    }
    
    public long getPhysical() {
        return physical;
    }
    
    public long getLogical() {
        return logical;
    }
    
    /**
     * 将TSO编码为64位长整型
     * 高42位存储物理时间戳，低22位存储逻辑计数器
     */
    public long encode() {
        return (physical << 22) | (logical & 0x3FFFFF);
    }
    
    /**
     * 从64位长整型解码TSO
     */
    public static TSO decode(long encoded) {
        long physical = encoded >>> 22;
        long logical = encoded & 0x3FFFFF;
        return new TSO(physical, logical);
    }
    
    /**
     * 比较两个TSO的大小
     */
    public int compareTo(TSO other) {
        if (this.physical != other.physical) {
            return Long.compare(this.physical, other.physical);
        }
        return Long.compare(this.logical, other.logical);
    }
    
    @Override
    public String toString() {
        return String.format("TSO{physical=%d, logical=%d, encoded=%d}", 
                           physical, logical, encode());
    }
}