package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.DataFetcherBlockReader;

import java.io.ByteArrayOutputStream;
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

    private static final byte[] buffer = new byte[8*1024];

    @Override
    public byte[] fetch(String key, long offset, int size) {
        try(InputStream is = client.fetch(key, offset, size)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(size);

            int sz;
            while ((sz = is.read(buffer)) != -1) {
                out.write(buffer, 0, sz);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
