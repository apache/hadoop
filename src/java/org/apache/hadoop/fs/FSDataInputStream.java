/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import java.io.*;
import java.util.Arrays;
import java.util.zip.*;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.StringUtils;

/** Utility that wraps a {@link FSInputStream} in a {@link DataInputStream}
 * and buffers input through a {@link BufferedInputStream}. */
public class FSDataInputStream extends DataInputStream
    implements Seekable, PositionedReadable {
  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.fs.DataInputStream");

  private static final byte[] VERSION = FSDataOutputStream.CHECKSUM_VERSION;
  private static final int HEADER_LENGTH = 8;
  
  private int bytesPerSum = 1;
  
  /** Verify that data matches checksums. */
  private class Checker extends FilterInputStream
      implements Seekable, PositionedReadable {
    private FileSystem fs;
    private Path file;
    private FSDataInputStream sums;
    private Checksum sum = new CRC32();
    private int inSum;

    public Checker(FileSystem fs, Path file, Configuration conf)
      throws IOException {
      super(fs.openRaw(file));
      
      this.fs = fs;
      this.file = file;
      Path sumFile = FileSystem.getChecksumFile(file);
      try {
        this.sums = new FSDataInputStream(fs.openRaw(sumFile), conf);
        byte[] version = new byte[VERSION.length];
        sums.readFully(version);
        if (!Arrays.equals(version, VERSION))
          throw new IOException("Not a checksum file: "+sumFile);
        bytesPerSum = sums.readInt();
      } catch (FileNotFoundException e) {         // quietly ignore
        stopSumming();
      } catch (IOException e) {                   // loudly ignore
        LOG.warn("Problem opening checksum file: "+ file + 
                 ".  Ignoring exception: " + 
                 StringUtils.stringifyException(e));
        stopSumming();
      }
    }

    public void seek(long desired) throws IOException {
      ((Seekable)in).seek(desired);
      if (sums != null) {
        if (desired % bytesPerSum != 0)
          throw new IOException("Seek to non-checksummed position.");
        try {
          sums.seek(HEADER_LENGTH + 4*(desired/bytesPerSum));
        } catch (IOException e) {
          LOG.warn("Problem seeking checksum file: "+e+". Ignoring.");
          stopSumming();
        }
        sum.reset();
        inSum = 0;
      }
    }
    
    public int read(byte b[], int off, int len) throws IOException {
      int read = in.read(b, off, len);

      if (sums != null) {
        int summed = 0;
        while (summed < read) {
          
          int goal = bytesPerSum - inSum;
          int inBuf = read - summed;
          int toSum = inBuf <= goal ? inBuf : goal;
          
          try {
            sum.update(b, off+summed, toSum);
          } catch (ArrayIndexOutOfBoundsException e) {
            throw new RuntimeException("Summer buffer overflow b.len=" + 
                                       b.length + ", off=" + off + 
                                       ", summed=" + summed + ", read=" + 
                                       read + ", bytesPerSum=" + bytesPerSum +
                                       ", inSum=" + inSum, e);
          }
          summed += toSum;
          
          inSum += toSum;
          if (inSum == bytesPerSum) {
            verifySum(read-(summed-bytesPerSum));
          }
        }
      }
        
      return read;
    }

    private void verifySum(int delta) throws IOException {
      int crc;
      try {
        crc = sums.readInt();
      } catch (IOException e) {
        LOG.warn("Problem reading checksum file: "+e+". Ignoring.");
        stopSumming();
        return;
      }
      int sumValue = (int)sum.getValue();
      sum.reset();
      inSum = 0;
      if (crc != sumValue) {
        long pos = getPos() - delta;
        fs.reportChecksumFailure(file, (FSInputStream)in,
                                 pos, bytesPerSum, crc);
        throw new ChecksumException("Checksum error: "+file+" at "+pos);
      }
    }

    public long getPos() throws IOException {
      return ((FSInputStream)in).getPos();
    }

    public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
      return ((FSInputStream)in).read(position, buffer, offset, length);
    }
    
    public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
      ((FSInputStream)in).readFully(position, buffer, offset, length);
    }
    
    public void readFully(long position, byte[] buffer)
    throws IOException {
      ((FSInputStream)in).readFully(position, buffer);
    }

    public void close() throws IOException {
      super.close();
      stopSumming();
    }

    private void stopSumming() {
      if (sums != null) {
        try {
          sums.close();
        } catch (IOException f) {}
        sums = null;
        bytesPerSum = 1;
      }
    }
  }

  /** Cache the file position.  This improves performance significantly.*/
  private static class PositionCache extends FilterInputStream {
    long position;

    public PositionCache(InputStream in) throws IOException {
      super(in);
    }

    // This is the only read() method called by BufferedInputStream, so we trap
    // calls to it in order to cache the position.
    public int read(byte b[], int off, int len) throws IOException {
      int result;
      if( (result = in.read(b, off, len)) > 0 )
        position += result;
      return result;
    }

    public void seek(long desired) throws IOException {
      ((Seekable)in).seek(desired);               // seek underlying stream
      position = desired;                         // update position
    }
      
    public long getPos() throws IOException {
      return position;                            // return cached position
    }
    
    public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
      return ((PositionedReadable)in).read(position, buffer, offset, length);
    }
    
    public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
      ((PositionedReadable)in).readFully(position, buffer, offset, length);
    }
    
  }

  /** Buffer input.  This improves performance significantly.*/
  private class Buffer extends BufferedInputStream {
    public Buffer(PositionCache in, int bufferSize)
      throws IOException {
      super(in, bufferSize);
    }

    public void seek(long desired) throws IOException {
      long end = ((PositionCache)in).getPos();
      long start = end - this.count;
      int avail = this.count - this.pos;
      if (desired >= start && desired < end && avail > 0) {
        this.pos = (int)(desired - start);        // can position within buffer
      } else {
        this.count = 0;                           // invalidate buffer
        this.pos = 0;

        long delta = desired % bytesPerSum;
        
        // seek to last checksummed point, if any
        ((PositionCache)in).seek(desired - delta);

        // scan to desired position
        for (int i = 0; i < delta; i++) {
          read();
        }
      }

    }
      
    public long getPos() throws IOException {     // adjust for buffer
      return ((PositionCache)in).getPos() - (this.count - this.pos);
    }

    // optimized version of read()
    public int read() throws IOException {
      if (pos >= count)
        return super.read();
      return buf[pos++] & 0xff;
    }

    public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
      return ((PositionCache)in).read(position, buffer, offset, length);
    }
    
    public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
      ((PositionCache)in).readFully(position, buffer, offset, length);
    }
}
  
  
  public FSDataInputStream(FileSystem fs, Path file, int bufferSize, Configuration conf)
      throws IOException {
    super(null);
    this.in = new Buffer(new PositionCache(new Checker(fs, file, conf)), bufferSize);
  }
  
  
  public FSDataInputStream(FileSystem fs, Path file, Configuration conf)
    throws IOException {
    super(null);
    int bufferSize = conf.getInt("io.file.buffer.size", 4096);
    this.in = new Buffer(new PositionCache(new Checker(fs, file, conf)), bufferSize);
  }
    
  /** Construct without checksums. */
  public FSDataInputStream(FSInputStream in, Configuration conf) throws IOException {
    this(in, conf.getInt("io.file.buffer.size", 4096));
  }
  /** Construct without checksums. */
  public FSDataInputStream(FSInputStream in, int bufferSize)
    throws IOException {
    super(null);
    this.in = new Buffer(new PositionCache(in), bufferSize);
  }
  
  public synchronized void seek(long desired) throws IOException {
    ((Buffer)in).seek(desired);
  }

  public long getPos() throws IOException {
    return ((Buffer)in).getPos();
  }

  public int read(long position, byte[] buffer, int offset, int length)
  throws IOException {
    return ((Buffer)in).read(position, buffer, offset, length);
  }
  
  public void readFully(long position, byte[] buffer, int offset, int length)
  throws IOException {
    ((Buffer)in).readFully(position, buffer, offset, length);
  }
  
  public void readFully(long position, byte[] buffer)
  throws IOException {
    ((Buffer)in).readFully(position, buffer, 0, buffer.length);
  }
}
