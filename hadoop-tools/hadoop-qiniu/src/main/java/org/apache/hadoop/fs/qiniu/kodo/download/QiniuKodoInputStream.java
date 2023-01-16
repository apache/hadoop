package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem.Statistics;
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
        LOG.debug("Read to buf array: offset={}, len={}", off, len);
        checkNotClosed();

        if (buf == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > buf.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        long endPosition = position + len;  // 结束位置
        int blockIdStart = (int) (position / blockSize); // 当前pos的blockId
        int blockIdEnd = (int) (endPosition / blockSize);    // 结束pos的blockId

        for(int blockId = blockIdStart; blockId < blockIdEnd; blockId++) {
            refreshCurrentBlock(); // 加载当前的块数据
            int start = (int)(position % blockSize);    // 计算块内起始位置
            System.arraycopy(currentBlockData, start,
                    buf, off + blockId * blockSize,
                    blockSize - start);
            // 循环次数
            position += blockSize - start;
        }

        refreshCurrentBlock();
        // 最后一块的数据
        int remainBlockSize = (int)(endPosition % blockSize);
        System.arraycopy(currentBlockData, 0,
                buf,off + blockIdEnd*blockSize,
                remainBlockSize);
        position += remainBlockSize;

        // 还能读
        if (position <= contentLength) {
            return len;
        }

        return -1;
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
