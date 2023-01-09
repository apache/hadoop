package org.apache.hadoop.fs.qiniu.kodo;

import com.qiniu.common.QiniuException;
import org.apache.hadoop.fs.FileAlreadyExistsException;
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

    private volatile QiniuException uploadException;

    public QiniuKodoOutputStream(QiniuKodoClient client, String key, String token) {
        this.key = key;
        this.pos = new PipedOutputStream();
        try {
            this.pis = new PipedInputStream(pos);
            this.thread = new Thread(() -> {
                try {
                    client.upload(pis, key, token);
                } catch (QiniuException e) {
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
    public void close() throws IOException {
        pos.close();
        try {
            thread.join();
            if (uploadException != null) {
                if (uploadException.response.statusCode == 614) {
                    throw new FileAlreadyExistsException("key exists " + key);
                }
                throw uploadException;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
