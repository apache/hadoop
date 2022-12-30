package org.apache.hadoop.fs.qiniu.kodo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;

class QiniuKodoBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoBuffer.class);

    private volatile boolean isClosed;

    private final byte[] bytes;

    private int readOffsetFull;

    // 已读的 byte 偏移量
    private int readOffset;

    private int writeOffsetFull;

    // 已写的 byte 偏移量
    private int writeOffset;

    QiniuKodoBuffer(int bufferSize) {
        if (bufferSize <= 0) {
            bufferSize = 8 * 1024;
        }
        this.isClosed = false;
        this.bytes = new byte[bufferSize];
        this.readOffset = -1;
        this.readOffsetFull = -1;
        this.writeOffset = -1;
        this.writeOffsetFull = -1;
    }

    // 读一个 byte, 0~255
    int read() throws IOException {
        for (; ; ) {
            synchronized (this) {
                if (hasByteToRead()) {
                    this.readOffset++;
                    this.readOffsetFull++;
                    if (this.readOffset >= bytes.length) {
                        this.readOffset = 0;
                    }
                    LOG.info("buff read:" + this.bytes[this.readOffset]);
                    return this.bytes[this.readOffset];
                }

                if (isClosed) {
                    LOG.info("buff read: but close");
                    return -1;
                }

                LOG.info("buff read: wait");
            }

            // 无数据可读，等待
            try {
                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // 写一个 byte, 0~255
    void write(int b) throws IOException {
        for (; ; ) {
            synchronized (this) {
                if (isClosed) {
                    LOG.info("buff write: but close");
                    throw new IOException("qiniu buffer is closed:" + this);
                }

                if (hasSpaceToWrite()) {
                    this.writeOffset++;
                    this.writeOffsetFull++;
                    if (this.writeOffset >= bytes.length) {
                        this.writeOffset = 0;
                    }
                    LOG.info("buff write:" + b);
                    this.bytes[this.writeOffset] = (byte) (b & 0xFF);
                }

                LOG.info("buff write: wait");
            }

            // 无地方可写，等待
            try {
                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    synchronized void close() {
        isClosed = true;
    }

    private boolean hasByteToRead() {
        changeFullOffsetIfNeeded();
        return this.readOffsetFull < this.writeOffsetFull;
    }

    private boolean hasSpaceToWrite() {
        changeFullOffsetIfNeeded();
        return (this.writeOffsetFull - this.readOffset) < bytes.length;
    }

    private void changeFullOffsetIfNeeded() {
        if (this.readOffsetFull < bytes.length || this.writeOffsetFull < bytes.length) {
            return;
        }
        this.readOffsetFull -= bytes.length;
        this.writeOffsetFull -= bytes.length;
    }
}
