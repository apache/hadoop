package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.io.SequenceFile;

public class SequenceFileLogReader implements HLog.Reader {
  
  /**
   * Hack just to set the correct file length up in SequenceFile.Reader.
   * See HADOOP-6307.  The below is all about setting the right length on the
   * file we are reading.  fs.getFileStatus(file).getLen() is passed down to
   * a private SequenceFile.Reader constructor.  This won't work.  Need to do
   * the available on the stream.  The below is ugly.  It makes getPos, the
   * first time its called, return length of the file -- i.e. tell a lie -- just
   * so this line up in SF.Reader's constructor ends up with right answer:
   * 
   *         this.end = in.getPos() + length;
   *
   */
  private static class WALReader extends SequenceFile.Reader {

    WALReader(final FileSystem fs, final Path p, final Configuration c)
    throws IOException {
      super(fs, p, c);
      
    }

    @Override
    protected FSDataInputStream openFile(FileSystem fs, Path file,
      int bufferSize, long length)
    throws IOException {
      return new WALReaderFSDataInputStream(super.openFile(fs, file, 
        bufferSize, length), length);
    }

    /**
     * Override just so can intercept first call to getPos.
     */
    static class WALReaderFSDataInputStream extends FSDataInputStream {
      private boolean firstGetPosInvocation = true;
      private long length;

      WALReaderFSDataInputStream(final FSDataInputStream is, final long l)
      throws IOException {
        super(is);
        this.length = l;
      }

      @Override
      public long getPos() throws IOException {
        if (this.firstGetPosInvocation) {
          this.firstGetPosInvocation = false;
          // Tell a lie.  We're doing this just so that this line up in
          // SequenceFile.Reader constructor comes out with the correct length
          // on the file:
          //         this.end = in.getPos() + length;
          long available = this.in.available();
          // Length gets added up in the SF.Reader constructor so subtract the
          // difference.  If available < this.length, then return this.length.
          return available >= this.length? available - this.length: this.length;
        }
        return super.getPos();
      }
    }
  }

  Configuration conf;
  WALReader reader;
  
  public SequenceFileLogReader() { }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf)
      throws IOException {
    this.conf = conf;
    reader = new WALReader(fs, path, conf);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public HLog.Entry next() throws IOException {
    return next(null);
  }

  @Override
  public HLog.Entry next(HLog.Entry reuse) throws IOException {
    if (reuse == null) {
      HLogKey key = HLog.newKey(conf);
      WALEdit val = new WALEdit();
      if (reader.next(key, val)) {
        return new HLog.Entry(key, val);
      }
    } else if (reader.next(reuse.getKey(), reuse.getEdit())) {
      return reuse;
    }
    return null;
  }

  @Override
  public void seek(long pos) throws IOException {
    reader.seek(pos);
  }

  @Override
  public long getPosition() throws IOException {
    return reader.getPosition();
  }

}
