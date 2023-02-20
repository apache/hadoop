package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.qiniu.kodo.blockcache.DiskCacheBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockManager;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.MemoryCacheBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.download.cache.DiskCacheConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.download.cache.MemoryCacheConfig;

import java.io.IOException;

public class QiniuKodoBlockReader implements IBlockReader, IBlockManager {

    private IBlockReader sourceReader = null;
    private DiskCacheBlockReader diskCacheReader = null;
    private MemoryCacheBlockReader memoryCacheReader = null;

    private IBlockReader finalReader = null;
    private final int blockSize;

    public QiniuKodoBlockReader(
            QiniuKodoFsConfig fsConfig,
            IQiniuKodoClient client
    ) throws IOException {
        int blockSize = fsConfig.download.blockSize;
        DiskCacheConfig diskCache = fsConfig.download.cache.disk;
        MemoryCacheConfig memoryCache = fsConfig.download.cache.memory;

        // 构造原始数据获取器
        this.sourceReader = new QiniuKodoSourceDataFetcher(blockSize, client);
        this.finalReader = sourceReader;

        if (diskCache.enable) {
            // 添加磁盘缓存层
            this.diskCacheReader = new DiskCacheBlockReader(
                    sourceReader,
                    diskCache.blocks,
                    diskCache.dir,
                    diskCache.expires
            );
            this.finalReader = this.diskCacheReader;
        }

        if (memoryCache.enable) {
            // 添加内存缓存
            this.memoryCacheReader = new MemoryCacheBlockReader(
                    diskCacheReader == null ? sourceReader : diskCacheReader,
                    memoryCache.blocks
            );
            this.finalReader = this.memoryCacheReader;
        }
        this.blockSize = finalReader.getBlockSize();
    }

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public byte[] readBlock(String key, int blockId) {
        return finalReader.readBlock(key, blockId);
    }

    @Override
    public void close() throws IOException {
        if (sourceReader != null) sourceReader.close();
        if (diskCacheReader != null) diskCacheReader.close();
        if (memoryCacheReader != null) memoryCacheReader.close();
    }

    @Override
    public void deleteBlocks(String key) {
        if (memoryCacheReader != null) memoryCacheReader.deleteBlocks(key);
        if (diskCacheReader != null) diskCacheReader.deleteBlocks(key);
    }
}
