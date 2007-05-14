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
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * Abstract Checksumed FileSystem.
 * It provide a basice implementation of a Checksumed FileSystem,
 * which creates a checksum file for each raw file.
 * It generates & verifies checksums at the client side.
 *
 * @author Hairong Kuang
 *****************************************************************/
public abstract class ChecksumFileSystem extends FilterFileSystem {
  private static final byte[] CHECKSUM_VERSION = new byte[] {'c', 'r', 'c', 0};

  public static double getApproxChkSumLength(long size) {
    return FSOutputSummer.CHKSUM_AS_FRACTION * size;
  }
  
  public ChecksumFileSystem(FileSystem fs) {
    super(fs);
  }

  /** get the raw file system */
  public FileSystem getRawFileSystem() {
    return fs;
  }

  /** Return the name of the checksum file associated with a file.*/
  public Path getChecksumFile(Path file) {
    return new Path(file.getParent(), "." + file.getName() + ".crc");
  }

  /** Return true iff file is a checksum file name.*/
  public static boolean isChecksumFile(Path file) {
    String name = file.getName();
    return name.startsWith(".") && name.endsWith(".crc");
  }

  /** Return the length of the checksum file given the size of the 
   * actual file.
   **/
  public long getChecksumFileLength(Path file, long fileSize) {
    return FSOutputSummer.getChecksumLength(fileSize, getBytesPerSum());
  }

  /** Return the bytes Per Checksum */
  public int getBytesPerSum() {
    return getConf().getInt("io.bytes.per.checksum", 512);
  }

  private int getSumBufferSize(int bytesPerSum, int bufferSize) {
    int defaultBufferSize = getConf().getInt("io.file.buffer.size", 4096);
    int proportionalBufferSize = bufferSize / bytesPerSum;
    return Math.max(bytesPerSum,
                    Math.max(proportionalBufferSize, defaultBufferSize));
  }

  /*******************************************************
   * For open()'s FSInputStream
   * It verifies that data matches checksums.
   *******************************************************/
  private static class FSInputChecker extends FSInputStream {
    public static final Log LOG 
      = LogFactory.getLog("org.apache.hadoop.fs.FSInputChecker");
    
    private ChecksumFileSystem fs;
    private Path file;
    private FSDataInputStream datas;
    private FSDataInputStream sums;
    private Checksum sum = new CRC32();
    private int inSum;
    
    private static final int HEADER_LENGTH = 8;
    
    private int bytesPerSum = 1;
    
    public FSInputChecker(ChecksumFileSystem fs, Path file)
      throws IOException {
      this(fs, file, fs.getConf().getInt("io.file.buffer.size", 4096));
    }
    
    public FSInputChecker(ChecksumFileSystem fs, Path file, int bufferSize)
      throws IOException {
      // open with an extremly small buffer size,
      // so that the buffer could be by-passed by the buffer in FSDataInputStream
      datas = fs.getRawFileSystem().open(file, 1);
      this.fs = fs;
      this.file = file;
      Path sumFile = fs.getChecksumFile(file);
      try {
        int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(), bufferSize);
        sums = fs.getRawFileSystem().open(sumFile, sumBufferSize);

        byte[] version = new byte[CHECKSUM_VERSION.length];
        sums.readFully(version);
        if (!Arrays.equals(version, CHECKSUM_VERSION))
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

    private long getChecksumFilePos( long dataPos ) {
      return HEADER_LENGTH + 4*(dataPos/bytesPerSum);
    }
    
    public void seek(long desired) throws IOException {
      // seek to a checksum boundary
      long checksumBoundary = desired/bytesPerSum*bytesPerSum;
      if (checksumBoundary != getPos()) {
        datas.seek(checksumBoundary);
        if (sums != null) {
          sums.seek(getChecksumFilePos(checksumBoundary));
        }
      }
      
      if (sums != null) {
        sum.reset();
        inSum = 0;
      }
      
      // scan to desired position
      int delta = (int)(desired - checksumBoundary);
      readBuffer(new byte[delta], 0, delta);
    }
    
    public int read() throws IOException {
      byte[] b = new byte[1];
      readBuffer(b, 0, 1);
      return b[0] & 0xff;
    }

    public int read(byte b[]) throws IOException {
      return read(b, 0, b.length);
    }

    public int read(byte b[], int off, int len) throws IOException {
      // make sure that it ends at a checksum boundary
      long curPos = getPos();
      long endPos = len+curPos/bytesPerSum*bytesPerSum;
      return readBuffer(b, off, (int)(endPos-curPos));
    }
    
    private int readBuffer(byte b[], int off, int len) throws IOException {
      int read;
      boolean retry;
      int retriesLeft = 3;
      long oldPos = getPos();
      do {
        retriesLeft--;
        retry = false;
        
        read = 0;
        boolean endOfFile=false;
        while (read < len && !endOfFile) {
          int count = datas.read(b, off + read, len - read);
          if (count < 0)
            endOfFile = true;
          else
            read += count;
        }
        
        if (sums != null && read!=0) {
          long oldSumsPos = sums.getPos();
          try {
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
              } else if (read == summed && endOfFile) {
                verifySum(read-read/bytesPerSum*bytesPerSum);
              }
            }
          } catch (ChecksumException ce) {
            LOG.info("Found checksum error: "+StringUtils.stringifyException(ce));
            long errPos = ce.getPos();
            boolean shouldRetry = fs.reportChecksumFailure(
                                                           file, datas, errPos, sums, errPos/bytesPerSum);
            if (!shouldRetry || retriesLeft == 0) {
              throw ce;
            }
            
            if (seekToNewSource(oldPos)) {
              // Since at least one of the sources is different, 
              // the read might succeed, so we'll retry.
              retry = true;
              seek(oldPos); //make sure Checksum sum's value gets restored
            } else {
              // Neither the data stream nor the checksum stream are being read
              // from different sources, meaning we'll still get a checksum error 
              // if we try to do the read again.  We throw an exception instead.
              throw ce;
            }
          }
        }
      } while (retry);
      return read==0?-1:read;
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
        throw new ChecksumException("Checksum error: "+file+" at "+pos, pos);
      }
    }
    
    public long getPos() throws IOException {
      return datas.getPos();
    }
    
    public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
      return datas.read(position, buffer, offset, length);
    }
    
    public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
      datas.readFully(position, buffer, offset, length);
    }
    
    public void readFully(long position, byte[] buffer)
      throws IOException {
      datas.readFully(position, buffer);
    }
    
    public void close() throws IOException {
      datas.close();
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
    
    public int available() throws IOException {
      return datas.available();
    }
    
    public boolean markSupported() {
      return datas.markSupported();
    }
    
    public synchronized void mark(int readlimit) {
      datas.mark(readlimit);
    }
    
    public synchronized void reset() throws IOException {
      datas.reset();
    }
    
    public long skip(long n) throws IOException {
      return datas.skip(n);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      boolean newDataSource = datas.seekToNewSource(targetPos);
      return sums.seekToNewSource(getChecksumFilePos(targetPos)) || newDataSource;
    }

  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (!exists(f)) {
      throw new FileNotFoundException(f.toString());
    }
    return new FSDataInputStream(new FSInputChecker(this, f, bufferSize),
                                 bufferSize);
  }

  /** This class provides an output stream for a checksummed file.
   * It generates checksums for data. */
  private static class FSOutputSummer extends FilterOutputStream {
    
    private FSDataOutputStream sums;
    private Checksum sum = new CRC32();
    private int inSum;
    private int bytesPerSum;
    private static final float CHKSUM_AS_FRACTION = 0.01f;
    
    public FSOutputSummer(ChecksumFileSystem fs, 
                          Path file, 
                          boolean overwrite, 
                          short replication,
                          long blockSize,
                          Configuration conf)
      throws IOException {
      this(fs, file, overwrite, 
           conf.getInt("io.file.buffer.size", 4096),
           replication, blockSize, null);
    }
    
    public FSOutputSummer(ChecksumFileSystem fs, 
                          Path file, 
                          boolean overwrite,
                          int bufferSize,
                          short replication,
                          long blockSize,
                          Progressable progress)
      throws IOException {
      super(fs.getRawFileSystem().create(file, overwrite, 1, 
                                         replication, blockSize, progress));
      this.bytesPerSum = fs.getBytesPerSum();
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
      this.sums = fs.getRawFileSystem().create(fs.getChecksumFile(file), true, 
                                               sumBufferSize, replication,
                                               blockSize);
      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(this.bytesPerSum);
    }
    
    public void write(byte b[], int off, int len) throws IOException {
      int summed = 0;
      while (summed < len) {
        
        int goal = this.bytesPerSum - inSum;
        int inBuf = len - summed;
        int toSum = inBuf <= goal ? inBuf : goal;
        
        sum.update(b, off+summed, toSum);
        summed += toSum;
        
        inSum += toSum;
        if (inSum == this.bytesPerSum) {
          writeSum();
        }
      }
      
      out.write(b, off, len);
    }
    
    private void writeSum() throws IOException {
      if (inSum != 0) {
        sums.writeInt((int)sum.getValue());
        sum.reset();
        inSum = 0;
      }
    }
    
    public void close() throws IOException {
      writeSum();
      if (sums != null) {
        sums.close();
      }
      out.close();
    }
    
    public static long getChecksumLength(long size, int bytesPerSum) {
      //the checksum length is equal to size passed divided by bytesPerSum +
      //bytes written in the beginning of the checksum file.  
      return ((long)(Math.ceil((float)size/bytesPerSum)) + 1) * 4 + 
        CHECKSUM_VERSION.length;  
    }
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file. 
   */
  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress)
    throws IOException {
    if (exists(f) && !overwrite) {
      throw new IOException("File already exists:" + f);
    }
    Path parent = f.getParent();
    if (parent != null && !mkdirs(parent)) {
      throw new IOException("Mkdirs failed to create " + parent);
    }
    return new FSDataOutputStream(new FSOutputSummer(this, f, overwrite,
                                                     bufferSize, replication, blockSize, progress), bufferSize);
  }

  /**
   * Set replication for an existing file.
   * Implement the abstract <tt>setReplication</tt> of <tt>FileSystem</tt>
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(Path src, short replication) throws IOException {
    boolean value = fs.setReplication(src, replication);
    if (!value)
      return false;

    Path checkFile = getChecksumFile(src);
    if (exists(checkFile))
      fs.setReplication(checkFile, replication);

    return true;
  }

  /**
   * Rename files/dirs
   */
  public boolean rename(Path src, Path dst) throws IOException {
    if (fs.isDirectory(src)) {
      return fs.rename(src, dst);
    } else {

      boolean value = fs.rename(src, dst);
      if (!value)
        return false;

      Path checkFile = getChecksumFile(src);
      if (fs.exists(checkFile)) { //try to rename checksum
        if (fs.isDirectory(dst)) {
          value = fs.rename(checkFile, dst);
        } else {
          value = fs.rename(checkFile, getChecksumFile(dst));
        }
      }

      return value;
    }
  }

  /**
   * Get rid of Path f, whether a true file or dir.
   */
  public boolean delete(Path f) throws IOException {
    if (fs.isDirectory(f)) {
      return fs.delete(f);
    } else {
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.delete(checkFile);
      }

      return fs.delete(f);
    }
  }

  final private static PathFilter DEFAULT_FILTER = new PathFilter() {
      public boolean accept(Path file) {
        return !isChecksumFile(file);
      }
    };

  /** 
   * Filter raw files in the given pathes using the default checksum filter. 
   * @param files a list of paths
   * @return a list of files under the source paths
   * @exception IOException
   */
  @Override
  public Path[] listPaths(Path[] files) throws IOException {
    return fs.listPaths(files, DEFAULT_FILTER);
  }

  /** 
   * Filter raw files in the given path using the default checksum filter. 
   * @param f source path
   * @return a list of files under the source path
   * @exception IOException
   */
  public Path[] listPaths(Path f) throws IOException {
    return fs.listPaths(f, DEFAULT_FILTER);
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return fs.mkdirs(f);
  }

  @Override
  public void lock(Path f, boolean shared) throws IOException {
    if (fs.isDirectory(f)) {
      fs.lock(f, shared);
    } else {
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.lock(checkFile, shared);
      }
      fs.lock(f, shared);
    }
  }

  @Override
  public void release(Path f) throws IOException {
    if (fs.isDirectory(f)) {
      fs.release(f);
    } else {
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.release(getChecksumFile(f));
      }
      fs.release(f);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    FileSystem localFs = getNamed("file:///", getConf());
    FileUtil.copy(localFs, src, this, dst, delSrc, getConf());
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   */
  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    FileSystem localFs = getNamed("file:///", getConf());
    FileUtil.copy(this, src, localFs, dst, delSrc, getConf());
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * If src and dst are directories, the copyCrc parameter
   * determines whether to copy CRC files.
   */
  public void copyToLocalFile(Path src, Path dst, boolean copyCrc)
    throws IOException {
    if (!fs.isDirectory(src)) { // source is a file
      fs.copyToLocalFile(src, dst);
      FileSystem localFs = getNamed("file:///", getConf());
      if (localFs instanceof ChecksumFileSystem) {
        localFs = ((ChecksumFileSystem) localFs).getRawFileSystem();
      }
      if (localFs.isDirectory(dst)) {
        dst = new Path(dst, src.getName());
      }
      dst = getChecksumFile(dst);
      if (localFs.exists(dst)) { //remove old local checksum file
        localFs.delete(dst);
      }
      Path checksumFile = getChecksumFile(src);
      if (copyCrc && fs.exists(checksumFile)) { //copy checksum file
        fs.copyToLocalFile(checksumFile, dst);
      }
    } else {
      Path[] srcs = listPaths(src);
      for (Path srcFile : srcs) {
        copyToLocalFile(srcFile, new Path(dst, srcFile.getName()), copyCrc);
      }
    }
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  /**
   * Report a checksum error to the file system.
   * @param f the file name containing the error
   * @param in the stream open on the file
   * @param inPos the position of the beginning of the bad data in the file
   * @param sums the stream open on the checksum file
   * @param sumsPos the position of the beginning of the bad data in the checksum file
   * @return if retry is neccessary
   */
  public boolean reportChecksumFailure(Path f, FSDataInputStream in,
                                       long inPos, FSDataInputStream sums, long sumsPos) {
    return false;
  }
}
