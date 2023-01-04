package org.apache.hadoop.fs.qiniu.kodo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class QiniuKodoOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoOutputStream.class);

    private final PipedOutputStream pos;
    private final PipedInputStream pis;

    private final Thread thread;

    private IOException uploadException;

    public QiniuKodoOutputStream(QiniuKodoClient client, String key, String token) {
        this.pos = new PipedOutputStream();
        try {
            this.pis = new PipedInputStream(pos);
            this.thread = new Thread(()->{
                try {
                    client.upload(pis, key, token);
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
    public void close() throws IOException {
        pos.close();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (uploadException != null) throw uploadException;
        }
    }
}
