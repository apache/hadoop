package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.DataFetcherBlockReader;

import java.io.IOException;
import java.io.InputStream;

public class QiniuKodoSourceDataFetcher extends DataFetcherBlockReader {
    private final QiniuKodoClient client;
    public QiniuKodoSourceDataFetcher(
            int blockSize,
            QiniuKodoClient client) {
        super(blockSize);
        this.client = client;
    }

    @Override
    public byte[] fetch(String key, long from, long to) {
        try(InputStream is = client.fetch(key, from, to)) {
            return new byte[is.available()];
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
