package org.apache.hadoop.fs.qiniu.kodo;

import java.io.EOFException;
import java.io.IOException;

class QiniuKodoBuffer {

    private volatile boolean isClosed;

    private final byte[] bytes;

    private int readOffsetFull;
    private int readOffset;

    private int writeOffsetFull;
    private int writeOffset;

    QiniuKodoBuffer(int bufferSize) {
        if (bufferSize <= 0) {
            bufferSize = 8 * 1024;
        }
        this.isClosed = false;
        this.bytes = new byte[bufferSize];
        this.readOffset = 0;
        this.readOffsetFull = 0;
        this.writeOffset = 0;
        this.writeOffsetFull = 0;
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
                    return this.bytes[this.readOffset];
                }

                if (isClosed) {
                    return -1;
                }
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
                    throw new IOException("qiniu buffer is closed:" + this);
                }

                if (hasSpaceToWrite()) {
                    this.writeOffset++;
                    this.writeOffsetFull++;
                    if (this.writeOffset >= bytes.length) {
                        this.writeOffset = 0;
                    }
                    this.bytes[this.writeOffset] = (byte) (b & 0xFF);
                }
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
