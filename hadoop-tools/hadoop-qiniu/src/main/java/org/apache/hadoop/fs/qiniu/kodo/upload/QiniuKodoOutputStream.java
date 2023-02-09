package org.apache.hadoop.fs.qiniu.kodo.upload;

import com.qiniu.common.QiniuException;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockManager;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class QiniuKodoOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoOutputStream.class);

    private final String key;
    private final PipedOutputStream pos;
    private final PipedInputStream pis;

    private final Thread thread;

    private volatile IOException uploadException;


    public QiniuKodoOutputStream(
            IQiniuKodoClient client,
            String key,
            boolean overwrite,
            IBlockManager blockManager,
            int bufferSize
    ) {
        this.key = key;
        this.pos = new PipedOutputStream();
        try {
            this.pis = new PipedInputStream(pos, bufferSize);
            this.thread = new Thread(() -> {
                try {
                    client.upload(pis, key, overwrite);
                } catch (IOException e) {
                    this.uploadException = e;
                }
            });
            this.thread.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(int b) throws IOException {
        pos.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        pos.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        pos.close();
        try {
            thread.join();
            if (uploadException == null) {
                // 无异常退出
                return;
            }
            if (uploadException instanceof QiniuException &&
                    ((QiniuException) uploadException).response.statusCode == 614) {
                throw new FileAlreadyExistsException("key exists " + key);
            }

            throw uploadException;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
