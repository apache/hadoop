package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.DataFetcherBlockReader;

import java.io.ByteArrayOutputStream;
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
        try(InputStream is = client.fetch(key, offset, size)) {
            byte[] buf = new byte[size];
            int cnt = IOUtils.read(is, buf);
            return Arrays.copyOf(buf, cnt);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
