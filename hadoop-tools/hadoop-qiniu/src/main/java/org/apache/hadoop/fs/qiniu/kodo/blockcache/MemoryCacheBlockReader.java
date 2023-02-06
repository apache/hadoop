package org.apache.hadoop.fs.qiniu.kodo.blockcache;

import org.apache.hadoop.fs.qiniu.kodo.util.LRUCache;

import java.io.IOException;

public class MemoryCacheBlockReader implements IBlockReader, IBlockManager {
    private final IBlockReader source;
    private final LRUCache<KeyBlockIdCacheKey, byte[]> lruCache;
    private final int blockSize;

    public MemoryCacheBlockReader(IBlockReader source, int maxCacheBlocks) {
        this.source = source;
        this.lruCache = new LRUCache<>(maxCacheBlocks);
        this.blockSize = source.getBlockSize();
    }


    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public byte[] readBlock(String key, int blockId) {
        KeyBlockIdCacheKey kbck = KeyBlockIdCacheKey.get(key, blockId);
        if (lruCache.containsKey(kbck)) return lruCache.get(kbck);
        byte[] blockData = source.readBlock(key, blockId);
        lruCache.put(kbck, blockData);
        return blockData;
    }

    @Override
    public void close() throws IOException {
        lruCache.clear();
    }

    @Override
    public void deleteBlocks(String key) {
        for (KeyBlockIdCacheKey kbck : lruCache.keySet()) {
            if (kbck.key.equals(key)) {
                lruCache.remove(kbck);
            }
        }
    }
}
