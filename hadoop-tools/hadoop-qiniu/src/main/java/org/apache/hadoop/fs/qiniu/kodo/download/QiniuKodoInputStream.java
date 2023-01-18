package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class QiniuKodoInputStream extends FSInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoInputStream.class);
    private final String key;


    private long position = 0;

    private final IBlockReader reader;
    private final int blockSize;

    private final long contentLength;
    private boolean closed = false;

    private int currentBlockId;
    private byte[] currentBlockData;

    private final Statistics statistics;

    public QiniuKodoInputStream(
            String key,
            IBlockReader reader,
            long contentLength,
            Statistics statistics) {
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
        return (int)remaining;
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        checkNotClosed();
        this.position = pos;
        refreshCurrentBlock();
    }

    @Override
    public synchronized long getPos() throws IOException {
        checkNotClosed();
        return position;
    }

    /**
     * 根据当前的 position 刷新缓存的 block 块
     */
    private void refreshCurrentBlock() {
        int blockId = (int)(position / (long) blockSize);

        // 单块缓存, blockId不变直接走缓存
        if (currentBlockData == null || blockId != currentBlockId) {
            // 未获取到 blockData 或 blockId 变了
            currentBlockId = blockId;
            currentBlockData = reader.readBlock(this.key, blockId);
            LOG.debug("读取数据块id: {}", currentBlockId);
        }
    }

    int byteReadCount = 0;

    @Override
    public synchronized int read() throws IOException {
        checkNotClosed();

        refreshCurrentBlock();
        int offset = (int)(position % (long) blockSize);
        if (currentBlockData.length < blockSize && offset >= currentBlockData.length) {
            LOG.debug("read position: {} eof", position);
            return -1;
        }
        position++;

        // 统计数据上报
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


        int offset = (int)(position % (long) blockSize);
        refreshCurrentBlock();
        if (currentBlockData.length < blockSize && offset >= currentBlockData.length) {
            return -1;
        }
        position++;
        int c = currentBlockData[offset];

        buf[off] = (byte)c;

        int i = 1;
        while (i < len) {
            offset = (int)(position % (long) blockSize);
            refreshCurrentBlock();
            if (currentBlockData.length < blockSize && offset >= currentBlockData.length) {
                break;
            }
            position++;
            c = currentBlockData[offset];

            buf[off + i] = (byte)c;
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

        // 统计数据上报
        if (statistics != null && byteReadCount > 0) {
            statistics.incrementBytesRead(byteReadCount);
            byteReadCount = 0;
        }
        closed = true;
        currentBlockData = null;
    }
}
