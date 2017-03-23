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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

/**
 * Abstract Checksumed Fs.
 * It provide a basic implementation of a Checksumed Fs,
 * which creates a checksum file for each raw file.
 * It generates & verifies checksums at the client side.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public abstract class ChecksumFs extends FilterFs {
  private static final byte[] CHECKSUM_VERSION = new byte[] {'c', 'r', 'c', 0};
  private int defaultBytesPerChecksum = 512;
  private boolean verifyChecksum = true;

  public static double getApproxChkSumLength(long size) {
    return ChecksumFSOutputSummer.CHKSUM_AS_FRACTION * size;
  }
  
  public ChecksumFs(AbstractFileSystem theFs)
    throws IOException, URISyntaxException {
    super(theFs);
    defaultBytesPerChecksum = 
      getMyFs().getServerDefaults(new Path("/")).getBytesPerChecksum();
  }
  
  /**
   * Set whether to verify checksum.
   */
  @Override
  public void setVerifyChecksum(boolean inVerifyChecksum) {
    this.verifyChecksum = inVerifyChecksum;
  }

  /** get the raw file system. */
  public AbstractFileSystem getRawFs() {
    return getMyFs();
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
    return getChecksumLength(fileSize, getBytesPerSum());
  }

  /** Return the bytes Per Checksum. */
  public int getBytesPerSum() {
    return defaultBytesPerChecksum;
  }

  private int getSumBufferSize(int bytesPerSum, int bufferSize, Path file)
    throws IOException {
    int defaultBufferSize = getMyFs().getServerDefaults(file)
        .getFileBufferSize();
    int proportionalBufferSize = bufferSize / bytesPerSum;
    return Math.max(bytesPerSum,
                    Math.max(proportionalBufferSize, defaultBufferSize));
  }

  /*******************************************************
   * For open()'s FSInputStream
   * It verifies that data matches checksums.
   *******************************************************/
  private static class ChecksumFSInputChecker extends FSInputChecker {
    public static final Log LOG 
      = LogFactory.getLog(FSInputChecker.class);
    private static final int HEADER_LENGTH = 8;
    
    private ChecksumFs fs;
    private FSDataInputStream datas;
    private FSDataInputStream sums;
    private int bytesPerSum = 1;
    private long fileLen = -1L;
    
    public ChecksumFSInputChecker(ChecksumFs fs, Path file)
      throws IOException, UnresolvedLinkException {
      this(fs, file, fs.getServerDefaults(file).getFileBufferSize());
    }
    
    public ChecksumFSInputChecker(ChecksumFs fs, Path file, int bufferSize)
      throws IOException, UnresolvedLinkException {
      super(file, fs.getFileStatus(file).getReplication());
      this.datas = fs.getRawFs().open(file, bufferSize);
      this.fs = fs;
      Path sumFile = fs.getChecksumFile(file);
      try {
        int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(),
            bufferSize, file);
        sums = fs.getRawFs().open(sumFile, sumBufferSize);

        byte[] version = new byte[CHECKSUM_VERSION.length];
        sums.readFully(version);
        if (!Arrays.equals(version, CHECKSUM_VERSION)) {
          throw new IOException("Not a checksum file: "+sumFile);
        }
        this.bytesPerSum = sums.readInt();
        set(fs.verifyChecksum, DataChecksum.newCrc32(), bytesPerSum, 4);
      } catch (FileNotFoundException e) {         // quietly ignore
        set(fs.verifyChecksum, null, 1, 0);
      } catch (IOException e) {                   // loudly ignore
        LOG.warn("Problem opening checksum file: "+ file + 
                 ".  Ignoring exception: " , e); 
        set(fs.verifyChecksum, null, 1, 0);
      }
    }
    
    private long getChecksumFilePos(long dataPos) {
      return HEADER_LENGTH + 4*(dataPos/bytesPerSum);
    }
    
    @Override
    protected long getChunkPosition(long dataPos) {
      return dataPos/bytesPerSum*bytesPerSum;
    }
    
    @Override
    public int available() throws IOException {
      return datas.available() + super.available();
    }
    
    @Override
    public int read(long position, byte[] b, int off, int len)
      throws IOException, UnresolvedLinkException {
      // parameter check
      if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      if (position<0) {
        throw new IllegalArgumentException(
            "Parameter position can not to be negative");
      }

      ChecksumFSInputChecker checker = new ChecksumFSInputChecker(fs, file);
      checker.seek(position);
      int nread = checker.read(b, off, len);
      checker.close();
      return nread;
    }
    
    @Override
    public void close() throws IOException {
      datas.close();
      if (sums != null) {
        sums.close();
      }
      set(fs.verifyChecksum, null, 1, 0);
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      final long sumsPos = getChecksumFilePos(targetPos);
      fs.reportChecksumFailure(file, datas, targetPos, sums, sumsPos);
      final boolean newDataSource = datas.seekToNewSource(targetPos);
      return sums.seekToNewSource(sumsPos) || newDataSource;
    }

    @Override
    protected int readChunk(long pos, byte[] buf, int offset, int len,
        byte[] checksum) throws IOException {
      boolean eof = false;
      if (needChecksum()) {
        assert checksum != null; // we have a checksum buffer
        assert checksum.length % CHECKSUM_SIZE == 0; // it is sane length
        assert len >= bytesPerSum; // we must read at least one chunk

        final int checksumsToRead = Math.min(
          len/bytesPerSum, // number of checksums based on len to read
          checksum.length / CHECKSUM_SIZE); // size of checksum buffer
        long checksumPos = getChecksumFilePos(pos); 
        if(checksumPos != sums.getPos()) {
          sums.seek(checksumPos);
        }

        int sumLenRead = sums.read(checksum, 0, CHECKSUM_SIZE * checksumsToRead);
        if (sumLenRead >= 0 && sumLenRead % CHECKSUM_SIZE != 0) {
          throw new EOFException("Checksum file not a length multiple of checksum size " +
                                 "in " + file + " at " + pos + " checksumpos: " + checksumPos +
                                 " sumLenread: " + sumLenRead );
        }
        if (sumLenRead <= 0) { // we're at the end of the file
          eof = true;
        } else {
          // Adjust amount of data to read based on how many checksum chunks we read
          len = Math.min(len, bytesPerSum * (sumLenRead / CHECKSUM_SIZE));
        }
      }
      if (pos != datas.getPos()) {
        datas.seek(pos);
      }
      int nread = readFully(datas, buf, offset, len);
      if (eof && nread > 0) {
        throw new ChecksumException("Checksum error: "+file+" at "+pos, pos);
      }
      return nread;
    }
    
    /* Return the file length */
    private long getFileLength() throws IOException, UnresolvedLinkException {
      if (fileLen==-1L) {
        fileLen = fs.getFileStatus(file).getLen();
      }
      return fileLen;
    }
    
    /**
     * Skips over and discards <code>n</code> bytes of data from the
     * input stream.
     *
     * The <code>skip</code> method skips over some smaller number of bytes
     * when reaching end of file before <code>n</code> bytes have been skipped.
     * The actual number of bytes skipped is returned.  If <code>n</code> is
     * negative, no bytes are skipped.
     *
     * @param      n   the number of bytes to be skipped.
     * @return     the actual number of bytes skipped.
     * @exception  IOException  if an I/O error occurs.
     *             ChecksumException if the chunk to skip to is corrupted
     */
    @Override
    public synchronized long skip(long n) throws IOException { 
      final long curPos = getPos();
      final long fileLength = getFileLength();
      if (n+curPos > fileLength) {
        n = fileLength - curPos;
      }
      return super.skip(n);
    }
    
    /**
     * Seek to the given position in the stream.
     * The next read() will be from that position.
     * 
     * <p>This method does not allow seek past the end of the file.
     * This produces IOException.
     *
     * @param      pos   the postion to seek to.
     * @exception  IOException  if an I/O error occurs or seeks after EOF
     *             ChecksumException if the chunk to seek to is corrupted
     */

    @Override
    public synchronized void seek(long pos) throws IOException { 
      if (pos>getFileLength()) {
        throw new IOException("Cannot seek after EOF");
      }
      super.seek(pos);
    }

  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) 
    throws IOException, UnresolvedLinkException {
    return new FSDataInputStream(
        new ChecksumFSInputChecker(this, f, bufferSize));
  }

  /**
   * Calculated the length of the checksum file in bytes.
   * @param size the length of the data file in bytes
   * @param bytesPerSum the number of bytes in a checksum block
   * @return the number of bytes in the checksum file
   */
  public static long getChecksumLength(long size, int bytesPerSum) {
    //the checksum length is equal to size passed divided by bytesPerSum +
    //bytes written in the beginning of the checksum file.  
    return ((size + bytesPerSum - 1) / bytesPerSum) * 4 +
             CHECKSUM_VERSION.length + 4;  
  }

  /** This class provides an output stream for a checksummed file.
   * It generates checksums for data. */
  private static class ChecksumFSOutputSummer extends FSOutputSummer {
    private FSDataOutputStream datas;    
    private FSDataOutputStream sums;
    private static final float CHKSUM_AS_FRACTION = 0.01f;
    private boolean isClosed = false;
    
    
    public ChecksumFSOutputSummer(final ChecksumFs fs, final Path file, 
      final EnumSet<CreateFlag> createFlag,
      final FsPermission absolutePermission, final int bufferSize,
      final short replication, final long blockSize, 
      final Progressable progress, final ChecksumOpt checksumOpt,
      final boolean createParent) throws IOException {
      super(DataChecksum.newDataChecksum(DataChecksum.Type.CRC32,
          fs.getBytesPerSum()));

      // checksumOpt is passed down to the raw fs. Unless it implements
      // checksum impelemts internally, checksumOpt will be ignored.
      // If the raw fs does checksum internally, we will end up with
      // two layers of checksumming. i.e. checksumming checksum file.
      this.datas = fs.getRawFs().createInternal(file, createFlag,
          absolutePermission, bufferSize, replication, blockSize, progress,
           checksumOpt,  createParent);
      
      // Now create the chekcsumfile; adjust the buffsize
      int bytesPerSum = fs.getBytesPerSum();
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize, file);
      this.sums = fs.getRawFs().createInternal(fs.getChecksumFile(file),
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
          absolutePermission, sumBufferSize, replication, blockSize, progress,
          checksumOpt, createParent);
      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(bytesPerSum);
    }
    
    @Override
    public void close() throws IOException {
      try {
        flushBuffer();
        sums.close();
        datas.close();
      } finally {
        isClosed = true;
      }
    }
    
    @Override
    protected void writeChunk(byte[] b, int offset, int len, byte[] checksum,
        int ckoff, int cklen)
      throws IOException {
      datas.write(b, offset, len);
      sums.write(checksum, ckoff, cklen);
    }

    @Override
    protected void checkClosed() throws IOException {
      if (isClosed) {
        throw new ClosedChannelException();
      }
    }
  }

  @Override
  public FSDataOutputStream createInternal(Path f,
      EnumSet<CreateFlag> createFlag, FsPermission absolutePermission,
      int bufferSize, short replication, long blockSize, Progressable progress,
      ChecksumOpt checksumOpt, boolean createParent) throws IOException {
    final FSDataOutputStream out = new FSDataOutputStream(
        new ChecksumFSOutputSummer(this, f, createFlag, absolutePermission,
            bufferSize, replication, blockSize, progress,
            checksumOpt,  createParent), null);
    return out;
  }

  /** Check if exists.
   * @param f source file
   */
  private boolean exists(Path f) 
    throws IOException, UnresolvedLinkException {
    try {
      return getMyFs().getFileStatus(f) != null;
    } catch (FileNotFoundException e) {
      return false;
    }
  }
  
  /** True iff the named path is a directory.
   * Note: Avoid using this method. Instead reuse the FileStatus 
   * returned by getFileStatus() or listStatus() methods.
   */
  private boolean isDirectory(Path f) 
    throws IOException, UnresolvedLinkException {
    try {
      return getMyFs().getFileStatus(f).isDirectory();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
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
  @Override
  public boolean setReplication(Path src, short replication)
    throws IOException, UnresolvedLinkException {
    boolean value = getMyFs().setReplication(src, replication);
    if (!value) {
      return false;
    }
    Path checkFile = getChecksumFile(src);
    if (exists(checkFile)) {
      getMyFs().setReplication(checkFile, replication);
    }
    return true;
  }

  /**
   * Rename files/dirs.
   */
  @Override
  public void renameInternal(Path src, Path dst) 
    throws IOException, UnresolvedLinkException {
    if (isDirectory(src)) {
      getMyFs().rename(src, dst);
    } else {
      getMyFs().rename(src, dst);

      Path checkFile = getChecksumFile(src);
      if (exists(checkFile)) { //try to rename checksum
        if (isDirectory(dst)) {
          getMyFs().rename(checkFile, dst);
        } else {
          getMyFs().rename(checkFile, getChecksumFile(dst));
        }
      }
    }
  }

  /**
   * Implement the delete(Path, boolean) in checksum
   * file system.
   */
  @Override
  public boolean delete(Path f, boolean recursive) 
    throws IOException, UnresolvedLinkException {
    FileStatus fstatus = null;
    try {
      fstatus = getMyFs().getFileStatus(f);
    } catch(FileNotFoundException e) {
      return false;
    }
    if (fstatus.isDirectory()) {
      //this works since the crcs are in the same
      //directories and the files. so we just delete
      //everything in the underlying filesystem
      return getMyFs().delete(f, recursive);
    } else {
      Path checkFile = getChecksumFile(f);
      if (exists(checkFile)) {
        getMyFs().delete(checkFile, true);
      }
      return getMyFs().delete(f, true);
    }
  }

  /**
   * Report a checksum error to the file system.
   * @param f the file name containing the error
   * @param in the stream open on the file
   * @param inPos the position of the beginning of the bad data in the file
   * @param sums the stream open on the checksum file
   * @param sumsPos the position of the beginning of the bad data in the
   *         checksum file
   * @return if retry is neccessary
   */
  public boolean reportChecksumFailure(Path f, FSDataInputStream in,
    long inPos, FSDataInputStream sums, long sumsPos) {
    return false;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException,
      UnresolvedLinkException {
    ArrayList<FileStatus> results = new ArrayList<FileStatus>();
    FileStatus[] listing = getMyFs().listStatus(f);
    if (listing != null) {
      for (int i = 0; i < listing.length; i++) {
        if (!isChecksumFile(listing[i].getPath())) {
          results.add(listing[i]);
        }
      }
    }
    return results.toArray(new FileStatus[results.size()]);
  }
}
