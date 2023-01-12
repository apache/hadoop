package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.DiskCacheBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.MemoryCacheBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.config.download.cache.DiskCacheConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.download.cache.MemoryCacheConfig;

import java.io.IOException;

public class QiniuKodoBlockReader implements IBlockReader {

    private final IBlockReader reader;
    public QiniuKodoBlockReader(
            QiniuKodoFsConfig fsConfig,
            QiniuKodoClient client
    ) throws IOException {
        int blockSize = fsConfig.download.blockSize;
        DiskCacheConfig diskCache = fsConfig.download.cache.disk;
        MemoryCacheConfig memoryCache = fsConfig.download.cache.memory;

        // 构造原始数据获取器
        IBlockReader reader = new QiniuKodoSourceDataFetcher(blockSize, client);

        if (diskCache.enable) {
            // 添加磁盘缓存层
            reader = new DiskCacheBlockReader(
                    reader,
                    diskCache.blocks,
                    diskCache.dir
            );
        }

        // 必须添加内存缓存层，否则单字节读取可能将不断读取文件块
        reader = new MemoryCacheBlockReader(
                reader,
                memoryCache.blocks
        );
        this.reader = reader;
    }

    @Override
    public int getBlockSize() {
        return reader.getBlockSize();
    }

    @Override
    public byte[] readBlock(String key, int blockId) {
        return reader.readBlock(key, blockId);
    }
}
