package org.apache.hadoop.fs.qiniu.kodo.download.blockreader;

import org.apache.hadoop.fs.qiniu.kodo.blockcache.DiskCacheBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.MemoryCacheBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.download.cache.DiskCacheConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.download.cache.MemoryCacheConfig;

import java.io.IOException;

public class QiniuKodoGeneralBlockReader implements IBlockReader {

    private IBlockReader finalReader;
    private final int blockSize;

    public QiniuKodoGeneralBlockReader(
            QiniuKodoFsConfig fsConfig,
            IQiniuKodoClient client
    ) throws IOException {
        int blockSize = fsConfig.download.blockSize;
        DiskCacheConfig diskCache = fsConfig.download.cache.disk;
        MemoryCacheConfig memoryCache = fsConfig.download.cache.memory;

        // Build the block reader chain
        this.finalReader = new QiniuKodoSourceBlockReader(blockSize, client);

        if (diskCache.enable) {
            this.finalReader = new DiskCacheBlockReader(
                    this.finalReader,
                    diskCache.blocks,
                    diskCache.dir,
                    diskCache.expires
            );
        }

        if (memoryCache.enable) {
            this.finalReader = new MemoryCacheBlockReader(
                    this.finalReader,
                    memoryCache.blocks
            );
        }
        this.blockSize = finalReader.getBlockSize();
    }

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public byte[] readBlock(String key, int blockId) throws IOException {
        return finalReader.readBlock(key, blockId);
    }

    @Override
    public void close() throws IOException {
        this.finalReader.close();
    }

    @Override
    public void deleteBlocks(String key) {
        finalReader.deleteBlocks(key);
    }
}
