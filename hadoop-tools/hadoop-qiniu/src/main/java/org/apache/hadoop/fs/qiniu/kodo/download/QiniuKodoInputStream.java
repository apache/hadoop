package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class QiniuKodoInputStream extends FSInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoInputStream.class);
    private final String key;


    private long position = 0;

    private final IBlockReader reader;


    public QiniuKodoInputStream(String key, IBlockReader reader) {
        this.key = key;
        this.reader = reader;
    }


    @Override
    public void seek(long pos) throws IOException {
        this.position = pos;
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }



    @Override
    public int read() throws IOException {
        int blkSz = reader.getBlockSize();
        int blockId = (int)(position / (long) blkSz);
        byte[] blockData = reader.readBlock(this.key, blockId);
        int offset = (int)(position % (long) blkSz);
        if (blockData.length < blkSz && offset >= blockData.length) {
            LOG.debug("read position: {} eof", position);
            return -1;
        }
        position++;

        return Byte.toUnsignedInt(blockData[offset]);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
}
