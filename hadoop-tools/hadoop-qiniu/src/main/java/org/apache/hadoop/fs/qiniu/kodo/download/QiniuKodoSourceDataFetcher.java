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
    public byte[] fetch(String key, long offset, int size) {
        try(InputStream is = new BufferedInputStream(client.fetch(key, offset, size))) {
            byte[] blockData = new byte[(int) size];

            int pos = 0;
            int b;
            while((b = is.read()) != -1) {
                blockData[pos++] = (byte) b;
            }

            // 读取到整块数据
            if (pos == getBlockSize()) return blockData;

            // 读取到的数据比整块数据少
            return Arrays.copyOf(blockData, pos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
