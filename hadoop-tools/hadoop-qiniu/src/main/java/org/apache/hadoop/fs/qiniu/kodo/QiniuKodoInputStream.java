package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;
import java.io.InputStream;

class QiniuKodoInputStream extends FSInputStream {

    private volatile boolean closed;

    private volatile long offset;

    private String url;
    private QiniuKodoClient client;

    private InputStream inputStream;

    QiniuKodoInputStream(QiniuKodoClient client, String url, int bufferSize) throws IOException {
        this.client = client;
        this.url = url;
        this.inputStream = open();
    }

    @Override
    public synchronized void seek(long l) throws IOException {
        offset = l;
        inputStream = open();
    }

    @Override
    public synchronized long getPos() throws IOException {
        return offset;
    }

    @Override
    public synchronized boolean seekToNewSource(long l) throws IOException {
        return false;
    }


    @Override
    public synchronized int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        closed = true;
    }

    private void throwExceptionIfInputStreamClosed() throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    private InputStream open() throws IOException {
        return client.get(url, offset, 0);
    }
}
