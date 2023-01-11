package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.DataFetcherBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IDataFetcher;

import java.io.IOException;
import java.io.InputStream;

public class QiniuKodoDataFetcher extends DataFetcherBlockReader {
    private final QiniuKodoClient client;
    private final String key;
    public QiniuKodoDataFetcher(
            int blockSize,
            QiniuKodoClient client,
            String key) {
        super(blockSize);
        this.client = client;
        this.key = key;
    }

    @Override
    public byte[] fetch(long from, long to) {
        try(InputStream is = client.fetch(key, from, to)) {
            return new byte[is.available()];
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
