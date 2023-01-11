package org.apache.hadoop.fs.qiniu.kodo.blockcache;

public class MemoryCacheBlockReader implements IBlockReader{
    private final IBlockReader source;
    private final LRUCache<Integer, byte[]> lruCache;

    public MemoryCacheBlockReader(IBlockReader source, int maxCacheBlocks) {
        this.source = source;
        this.lruCache = new LRUCache<>(maxCacheBlocks);
    }

    @Override
    public int getBlockSize() {
        return source.getBlockSize();
    }

    @Override
    public byte[] readBlockById(int blockId) {
        if (lruCache.containsKey(blockId)) return lruCache.get(blockId);
        byte[] blockData = source.readBlockById(blockId);
        lruCache.put(blockId, blockData);
        return blockData;
    }
}
