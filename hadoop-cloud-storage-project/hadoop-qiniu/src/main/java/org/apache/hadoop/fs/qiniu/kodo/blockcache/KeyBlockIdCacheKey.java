package org.apache.hadoop.fs.qiniu.kodo.blockcache;

import java.util.Objects;

public class KeyBlockIdCacheKey {
    String key;
    int blockId;

    private KeyBlockIdCacheKey(String key, int blockId) {
        this.key = key;
        this.blockId = blockId;
    }

    public static KeyBlockIdCacheKey get(String key, int blockId) {
        return new KeyBlockIdCacheKey(key, blockId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyBlockIdCacheKey that = (KeyBlockIdCacheKey) o;
        return blockId == that.blockId && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, blockId);
    }

    @Override
    public String toString() {
        return "KeyBlockIdCacheKey{" +
                "key='" + key + '\'' +
                ", blockId=" + blockId +
                '}';
    }
}