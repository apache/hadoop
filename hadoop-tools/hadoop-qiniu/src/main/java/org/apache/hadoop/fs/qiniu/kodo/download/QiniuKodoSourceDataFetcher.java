package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.DataFetcherBlockReader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class QiniuKodoSourceDataFetcher extends DataFetcherBlockReader {
    private final QiniuKodoClient client;
    public QiniuKodoSourceDataFetcher(
            int blockSize,
            QiniuKodoClient client) {
        super(blockSize);
        this.client = client;
    }

    @Override
    public byte[] fetch(String key, long begin, long size) {
        try(InputStream is = new BufferedInputStream(client.fetch(key, begin, size))) {
            byte[] blockData = new byte[getBlockSize()];
            int sz = is.read(blockData);

            // 读取到整块数据
            if (sz == getBlockSize()) return blockData;

            // 读取到的数据比整块数据少
            return Arrays.copyOf(blockData, sz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
