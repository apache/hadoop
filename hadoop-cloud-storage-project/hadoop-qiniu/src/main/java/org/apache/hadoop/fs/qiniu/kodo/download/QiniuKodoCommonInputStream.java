package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;

public class QiniuKodoCommonInputStream extends FSInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoCommonInputStream.class);
    private final String key;


    private long position = 0;

    private final IBlockReader reader;
    private final int blockSize;

    private final long contentLength;
    private boolean closed = false;

    private int currentBlockId;
    private byte[] currentBlockData;

    private final Statistics statistics;

    public QiniuKodoCommonInputStream(
            String key,
            IBlockReader reader,
            long contentLength,
            Statistics statistics
    ) {
        this.key = key;
        this.reader = reader;
        this.blockSize = reader.getBlockSize();
        this.contentLength = contentLength;
        this.statistics = statistics;
    }

    @Override
    public synchronized int available() throws IOException {
        checkNotClosed();
        long remaining = contentLength - position;
        if (remaining > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) remaining;
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        checkNotClosed();
        if (pos < 0) {
            throw new EOFException("Don't allow a negative seek: " + pos);
        } else {
            this.position = pos;
        }
    }

    @Override
    public synchronized long getPos() throws IOException {
        checkNotClosed();
        if (position < 0) {
            return 0;
        }
        if (position >= contentLength) {
            return contentLength - 1;
        }
        return position;
    }

    /**
     * Refresh the cached block according to the current position
     */
    private void refreshCurrentBlock() throws IOException {
        int blockId = (int) (position / (long) blockSize);

        // if currentBlockData is null or blockId changed, refresh currentBlockData
        if (currentBlockData == null || blockId != currentBlockId) {
            currentBlockId = blockId;
            currentBlockData = reader.readBlock(this.key, blockId);
            LOG.debug("Read block id: {}", currentBlockId);
        }
    }

    int byteReadCount = 0;

    @Override
    public synchronized int read() throws IOException {
        checkNotClosed();
        if (position < 0 || position >= contentLength) {
            return -1;
        }
        refreshCurrentBlock();
        int offset = (int) (position % (long) blockSize);
        if (currentBlockData.length < blockSize && offset >= currentBlockData.length) {
            LOG.debug("read position: {} eof", position);
            return -1;
        }
        position++;

        if (statistics != null) {
            byteReadCount++;
            if (byteReadCount > 10) {
                statistics.incrementBytesRead(byteReadCount);
                byteReadCount = 0;
            }
        }

        return Byte.toUnsignedInt(currentBlockData[offset]);
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws IOException {
        LOG.debug("Read to buf array: offset={}, len={}, pos={}", off, len, position);
        checkNotClosed();

        if (buf == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > buf.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }


        int offset = (int) (position % (long) blockSize);
        refreshCurrentBlock();
        if (currentBlockData.length < blockSize && offset >= currentBlockData.length) {
            return -1;
        }
        position++;
        int c = currentBlockData[offset];

        buf[off] = (byte) c;

        int i = 1;
        while (i < len) {
            offset = (int) (position % (long) blockSize);
            refreshCurrentBlock();
            if (currentBlockData.length < blockSize && offset >= currentBlockData.length) {
                break;
            }
            position++;
            c = currentBlockData[offset];

            buf[off + i] = (byte) c;
            i++;
        }

        statistics.incrementBytesRead(i);

        return i;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        checkNotClosed();
        return false;
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;

        if (statistics != null && byteReadCount > 0) {
            statistics.incrementBytesRead(byteReadCount);
            byteReadCount = 0;
        }
        closed = true;
        currentBlockData = null;
    }
}
