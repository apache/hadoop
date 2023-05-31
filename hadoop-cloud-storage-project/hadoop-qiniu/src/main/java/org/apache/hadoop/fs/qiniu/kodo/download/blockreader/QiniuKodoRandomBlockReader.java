package org.apache.hadoop.fs.qiniu.kodo.download.blockreader;

import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.MemoryCacheBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;

import java.io.IOException;

public class QiniuKodoRandomBlockReader implements IBlockReader {
    private final MemoryCacheBlockReader memoryCacheReader;
    private final int blockSize;

    public QiniuKodoRandomBlockReader(IQiniuKodoClient kodoClient, int blockSize, int maxCacheBlocks) {
        this.memoryCacheReader = new MemoryCacheBlockReader(
                new QiniuKodoSourceBlockReader(blockSize, kodoClient),
                maxCacheBlocks
        );
        this.blockSize = blockSize;
    }

    @Override
    public void deleteBlocks(String key) {
        memoryCacheReader.deleteBlocks(key);
    }

    @Override
    public int getBlockSize() {
        return this.blockSize;
    }

    @Override
    public byte[] readBlock(String key, int blockId) throws IOException {
        return this.memoryCacheReader.readBlock(key, blockId);
    }

    @Override
    public void close() throws IOException {
        this.memoryCacheReader.close();
    }
}
