package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.xerial.snappy.SnappyInputStream;

public class SnappyCompressionInputStream extends SnappyInputStream implements Seekable, PositionedReadable {

    public SnappyCompressionInputStream(InputStream in) throws IOException {
        super(in);
    }

    public InputStream getWrappedStream() {
        return in;
    }

    @Override
    public void seek(long pos) throws IOException {
        throw new IOException("seek not implemented");
    }

    @Override
    public long getPos() throws IOException {
        throw new IOException("getPos not implemented");
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        /*
        Preconditions.checkArgument(targetPos >= 0,
                "Cannot seek to negative offset.");

        try {
            boolean result = ((Seekable) in).seekToNewSource(targetPos);
            resetStreamOffset(targetPos);
            return result;
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("This stream does not support " +
                    "seekToNewSource.");
        }*/
        throw new IOException("seekToNewSource not implemented");
    }

    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
        throw new IOException("read in PositionedReadable not implemented");
    }

    /**
     * Read the specified number of bytes, from a given
     * position within a file. This does not
     * change the current offset of a file, and is thread-safe.
     */
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        throw new IOException("readFully in PositionedReadable not implemented");
    }

    /**
     * Read number of bytes equal to the length of the buffer, from a given
     * position within a file. This does not
     * change the current offset of a file, and is thread-safe.
     */
    public void readFully(long position, byte[] buffer) throws IOException {
        throw new IOException("readFully in PositionedReadable not implemented");
    }
}
