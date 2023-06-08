package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.qiniu.kodo.blockcache.IBlockReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class QiniuKodoInputStream extends FSInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoInputStream.class);
    private final boolean useRandomReader;
    private final FSInputStream generalStrategy;
    private final FSInputStream randomStrategy;
    private FSInputStream currentStrategy;
    private final String key;

    public QiniuKodoInputStream(
            String key,
            boolean useRandomReader,
            IBlockReader generalReader,
            IBlockReader randomReader,
            long contentLength,
            FileSystem.Statistics statistics
    ) {
        this.key = key;

        this.useRandomReader = useRandomReader;
        this.generalStrategy = new QiniuKodoCommonInputStream(key, generalReader, contentLength, statistics);
        this.randomStrategy = new QiniuKodoCommonInputStream(key, randomReader, contentLength, statistics);

        this.currentStrategy = generalStrategy;
        LOG.trace("File {} read strategy is general reader", key);
    }

    @Override
    public int available() throws IOException {
        return currentStrategy.available();
    }

    @Override
    public void seek(long pos) throws IOException {
        if (useRandomReader) {
            this.currentStrategy = randomStrategy;
            LOG.info("File {} read strategy switch to random reader", key);
        }
        currentStrategy.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return currentStrategy.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return currentStrategy.seekToNewSource(targetPos);
    }

    @Override
    public int read() throws IOException {
        return currentStrategy.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return currentStrategy.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        generalStrategy.close();
        randomStrategy.close();
    }
}
