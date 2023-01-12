package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.DiskCacheBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.MemoryCacheBlockReader;

import java.io.IOException;

public class QiniuKodoBlockReader implements IBlockReader {

    private final IBlockReader reader;
    public QiniuKodoBlockReader(
            QiniuKodoFsConfig fsConfig,
            QiniuKodoClient client
    ) throws IOException {
        int blockSize = fsConfig.getDownloadBlockSize();
        // 构造原始数据获取器
        IBlockReader reader = new QiniuKodoSourceDataFetcher(blockSize, client);

        if (fsConfig.getDiskCacheEnable()) {
            // 添加磁盘缓存层
            reader = new DiskCacheBlockReader(
                    reader,
                    fsConfig.getDiskCacheBlocks(),
                    fsConfig.getDiskCacheDir()
            );
        }

        if (fsConfig.getMemoryCacheEnable()) {
            // 添加内存缓存层
            reader = new MemoryCacheBlockReader(
                    reader,
                    fsConfig.getMemoryCacheBlocks()
            );
        }
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
