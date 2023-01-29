package org.apache.hadoop.fs.qiniu.kodo.download;

import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;

public class EmptyInputStream extends FSInputStream {
    @Override
    public int available() throws IOException {
        return 0;
    }

    @Override
    public void seek(long pos) throws IOException {

    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        return -1;
    }
}
