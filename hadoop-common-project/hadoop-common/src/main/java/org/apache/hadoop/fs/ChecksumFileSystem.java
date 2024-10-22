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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.IntFunction;
import java.util.zip.CRC32;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.impl.CombinedFileRange;
import org.apache.hadoop.fs.impl.FutureDataInputStreamBuilderImpl;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.LambdaUtils;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_STANDARD_OPTIONS;
import static org.apache.hadoop.fs.VectoredReadUtils.validateAndSortRanges;
import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;
import static org.apache.hadoop.fs.impl.StoreImplementationUtils.isProbeForSyncable;

/****************************************************************
 * Abstract Checksumed FileSystem.
 * It provide a basic implementation of a Checksumed FileSystem,
 * which creates a checksum file for each raw file.
 * It generates &amp; verifies checksums at the client side.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ChecksumFileSystem extends FilterFileSystem {
  private static final byte[] CHECKSUM_VERSION = new byte[] {'c', 'r', 'c', 0};
  private int bytesPerChecksum = 512;
  private boolean verifyChecksum = true;
  private boolean writeChecksum = true;

  public static double getApproxChkSumLength(long size) {
    return ChecksumFSOutputSummer.CHKSUM_AS_FRACTION * size;
  }

  public ChecksumFileSystem(FileSystem fs) {
    super(fs);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      bytesPerChecksum = conf.getInt(LocalFileSystemConfigKeys.LOCAL_FS_BYTES_PER_CHECKSUM_KEY,
		                     LocalFileSystemConfigKeys.LOCAL_FS_BYTES_PER_CHECKSUM_DEFAULT);
      Preconditions.checkState(bytesPerChecksum > 0,
          "bytes per checksum should be positive but was %s",
          bytesPerChecksum);
    }
  }

  /**
   * Set whether to verify checksum.
   */
  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  @Override
  public void setWriteChecksum(boolean writeChecksum) {
    this.writeChecksum = writeChecksum;
  }

  /** get the raw file system */
  @Override
  public FileSystem getRawFileSystem() {
    return fs;
  }

  /**
   * Return the name of the checksum file associated with a file.
   *
   * @param file the file path.
   * @return name of the checksum file associated with a file.
   */
  public Path getChecksumFile(Path file) {
    return new Path(file.getParent(), "." + file.getName() + ".crc");
  }

  /**
   * Return true if file is a checksum file name.
   *
   * @param file the file path.
   * @return if file is a checksum file true, not false.
   */
  public static boolean isChecksumFile(Path file) {
    String name = file.getName();
    return name.startsWith(".") && name.endsWith(".crc");
  }

  /**
   * Return the length of the checksum file given the size of the
   * actual file.
   *
   * @param file the file path.
   * @param fileSize file size.
   * @return checksum length.
   */
  public long getChecksumFileLength(Path file, long fileSize) {
    return getChecksumLength(fileSize, getBytesPerSum());
  }

  /**
   * Return the bytes Per Checksum.
   *
   * @return bytes per check sum.
   */
  public int getBytesPerSum() {
    return bytesPerChecksum;
  }

  private int getSumBufferSize(int bytesPerSum, int bufferSize) {
    int defaultBufferSize = getConf().getInt(
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY,
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT);
    int proportionalBufferSize = bufferSize / bytesPerSum;
    return Math.max(bytesPerSum,
                    Math.max(proportionalBufferSize, defaultBufferSize));
  }

  /*******************************************************
   * For open()'s FSInputStream
   * It verifies that data matches checksums.
   *******************************************************/
  private static class ChecksumFSInputChecker extends FSInputChecker implements
      IOStatisticsSource, StreamCapabilities {
    private ChecksumFileSystem fs;
    private FSDataInputStream datas;
    private FSDataInputStream sums;

    private static final int HEADER_LENGTH = 8;

    private int bytesPerSum = 1;
    private long fileLen = -1L;

    public ChecksumFSInputChecker(ChecksumFileSystem fs, Path file)
      throws IOException {
      this(fs, file, fs.getConf().getInt(
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY,
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT));
    }

    public ChecksumFSInputChecker(ChecksumFileSystem fs, Path file, int bufferSize)
      throws IOException {
      super( file, fs.getFileStatus(file).getReplication() );
      this.datas = fs.getRawFileSystem().open(file, bufferSize);
      this.fs = fs;
      Path sumFile = fs.getChecksumFile(file);
      try {
        int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(), bufferSize);
        sums = fs.getRawFileSystem().open(sumFile, sumBufferSize);

        byte[] version = new byte[CHECKSUM_VERSION.length];
        sums.readFully(version);
        if (!Arrays.equals(version, CHECKSUM_VERSION))
          throw new IOException("Not a checksum file: "+sumFile);
        this.bytesPerSum = sums.readInt();
        set(fs.verifyChecksum, DataChecksum.newCrc32(), bytesPerSum,
            FSInputChecker.CHECKSUM_SIZE);
      } catch (IOException e) {
        // mincing the message is terrible, but java throws permission
        // exceptions as FNF because that's all the method signatures allow!
        if (!(e instanceof FileNotFoundException) ||
            e.getMessage().endsWith(" (Permission denied)")) {
          LOG.warn("Problem opening checksum file: "+ file +
              ".  Ignoring exception: " , e);
        }
        set(fs.verifyChecksum, null, 1, 0);
      }
    }

    private long getChecksumFilePos( long dataPos ) {
      return HEADER_LENGTH + FSInputChecker.CHECKSUM_SIZE*(dataPos/bytesPerSum);
    }

    @Override
    protected long getChunkPosition( long dataPos ) {
      return dataPos/bytesPerSum*bytesPerSum;
    }

    @Override
    public int available() throws IOException {
      return datas.available() + super.available();
    }

    @Override
    public int read(long position, byte[] b, int off, int len)
      throws IOException {
      // parameter check
      validatePositionedReadArgs(position, b, off, len);
      if (len == 0) {
        return 0;
      }

      int nread;
      try (ChecksumFSInputChecker checker =
               new ChecksumFSInputChecker(fs, file)) {
        checker.seek(position);
        nread = checker.read(b, off, len);
      }
      return nread;
    }

    @Override
    public void close() throws IOException {
      datas.close();
      if( sums != null ) {
        sums.close();
      }
      set(fs.verifyChecksum, null, 1, 0);
    }


    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      long sumsPos = getChecksumFilePos(targetPos);
      fs.reportChecksumFailure(file, datas, targetPos, sums, sumsPos);
      boolean newDataSource = datas.seekToNewSource(targetPos);
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
          throw new ChecksumException(
            "Checksum file not a length multiple of checksum size " +
            "in " + file + " at " + pos + " checksumpos: " + checksumPos +
            " sumLenread: " + sumLenRead,
            pos);
        }
        if (sumLenRead <= 0) { // we're at the end of the file
          eof = true;
        } else {
          // Adjust amount of data to read based on how many checksum chunks we read
          len = Math.min(len, bytesPerSum * (sumLenRead / CHECKSUM_SIZE));
        }
      }
      if(pos != datas.getPos()) {
        datas.seek(pos);
      }
      int nread = readFully(datas, buf, offset, len);
      if (eof && nread > 0) {
        throw new ChecksumException("Checksum error: "+file+" at "+pos, pos);
      }
      return nread;
    }

    /**
     * Get the IO Statistics of the nested stream, falling back to
     * null if the stream does not implement the interface
     * {@link IOStatisticsSource}.
     * @return an IOStatistics instance or null
     */
    @Override
    public IOStatistics getIOStatistics() {
      return IOStatisticsSupport.retrieveIOStatistics(datas);
    }

    public static long findChecksumOffset(long dataOffset,
                                          int bytesPerSum) {
      return HEADER_LENGTH + (dataOffset/bytesPerSum) * FSInputChecker.CHECKSUM_SIZE;
    }

    /**
     * Calculate length of file if not already cached.
     * @return file length.
     * @throws IOException any IOE.
     */
    private long getFileLength() throws IOException {
      if (fileLen == -1L) {
        fileLen = fs.getFileStatus(file).getLen();
      }
      return fileLen;
    }

    /**
     * Find the checksum ranges that correspond to the given data ranges.
     * @param dataRanges the input data ranges, which are assumed to be sorted
     *                   and non-overlapping
     * @return a list of AsyncReaderUtils.CombinedFileRange that correspond to
     *         the checksum ranges
     */
    public static List<CombinedFileRange> findChecksumRanges(
        List<? extends FileRange> dataRanges,
        int bytesPerSum,
        int minSeek,
        int maxSize) {
      List<CombinedFileRange> result = new ArrayList<>();
      CombinedFileRange currentCrc = null;
      for(FileRange range: dataRanges) {
        long crcOffset = findChecksumOffset(range.getOffset(), bytesPerSum);
        long crcEnd = findChecksumOffset(range.getOffset() + range.getLength() +
                                             bytesPerSum - 1, bytesPerSum);
        if (currentCrc == null ||
                !currentCrc.merge(crcOffset, crcEnd, range, minSeek, maxSize)) {
          currentCrc = new CombinedFileRange(crcOffset, crcEnd, range);
          result.add(currentCrc);
        }
      }
      return result;
    }

    /**
     * Check the data against the checksums.
     * @param sumsBytes the checksum data
     * @param sumsOffset where from the checksum file this buffer started
     * @param data the file data
     * @param dataOffset where the file data started (must be a multiple of
     *                  bytesPerSum)
     * @param bytesPerSum how many bytes per a checksum
     * @param file the path of the filename
     * @return the data buffer
     * @throws CompletionException if the checksums don't match
     */
    static ByteBuffer checkBytes(ByteBuffer sumsBytes,
                                 long sumsOffset,
                                 ByteBuffer data,
                                 long dataOffset,
                                 int bytesPerSum,
                                 Path file) {
      // determine how many bytes we need to skip at the start of the sums
      int offset =
          (int) (findChecksumOffset(dataOffset, bytesPerSum) - sumsOffset);
      IntBuffer sums = sumsBytes.asIntBuffer();
      sums.position(offset / FSInputChecker.CHECKSUM_SIZE);
      ByteBuffer current = data.duplicate();
      int numFullChunks = data.remaining() / bytesPerSum;
      boolean partialChunk = ((data.remaining() % bytesPerSum) != 0);
      int totalChunks = numFullChunks;
      if (partialChunk) {
        totalChunks++;
      }
      CRC32 crc = new CRC32();
      // check each chunk to ensure they match
      for(int c = 0; c < totalChunks; ++c) {
        // set the buffer position to the start of every chunk.
        current.position(c * bytesPerSum);

        if (c == numFullChunks) {
          // During last chunk, there may be less than chunk size
          // data preset, so setting the limit accordingly.
          int lastIncompleteChunk = data.remaining() % bytesPerSum;
          current.limit((c * bytesPerSum) + lastIncompleteChunk);
        } else {
          // set the buffer limit to end of every chunk.
          current.limit((c + 1) * bytesPerSum);
        }

        // compute the crc
        crc.reset();
        crc.update(current);
        int expected = sums.get();
        int calculated = (int) crc.getValue();

        if (calculated != expected) {
          // cast of c added to silence findbugs
          long errPosn = dataOffset + (long) c * bytesPerSum;
          throw new CompletionException(new ChecksumException(
              "Checksum error: " + file + " at " + errPosn +
                  " exp: " + expected + " got: " + calculated, errPosn));
        }
      }
      // if everything matches, we return the data
      return data;
    }

    /**
     * Vectored read.
     * If the file has no checksums: delegate to the underlying stream.
     * If the file is checksummed: calculate the checksum ranges as
     * well as the data ranges, read both, and validate the checksums
     * as well as returning the data.
     * @param ranges the byte ranges to read
     * @param allocate the function to allocate ByteBuffer
     * @throws IOException
     */
    @Override
    public void readVectored(List<? extends FileRange> ranges,
                             IntFunction<ByteBuffer> allocate) throws IOException {

      // If the stream doesn't have checksums, just delegate.
      if (sums == null) {
        datas.readVectored(ranges, allocate);
        return;
      }
      final long length = getFileLength();
      final List<? extends FileRange> sorted = validateAndSortRanges(ranges,
          Optional.of(length));
      int minSeek = minSeekForVectorReads();
      int maxSize = maxReadSizeForVectorReads();
      List<CombinedFileRange> dataRanges =
          VectoredReadUtils.mergeSortedRanges(sorted, bytesPerSum,
              minSeek, maxReadSizeForVectorReads());
      // While merging the ranges above, they are rounded up based on the value of bytesPerSum
      // which leads to some ranges crossing the EOF thus they need to be fixed else it will
      // cause EOFException during actual reads.
      for (CombinedFileRange range : dataRanges) {
        if (range.getOffset() + range.getLength() > length) {
          range.setLength((int) (length - range.getOffset()));
        }
      }
      List<CombinedFileRange> checksumRanges = findChecksumRanges(dataRanges,
          bytesPerSum, minSeek, maxSize);
      sums.readVectored(checksumRanges, allocate);
      datas.readVectored(dataRanges, allocate);
      for(CombinedFileRange checksumRange: checksumRanges) {
        for(FileRange dataRange: checksumRange.getUnderlying()) {
          // when we have both the ranges, validate the checksum
          CompletableFuture<ByteBuffer> result =
              checksumRange.getData().thenCombineAsync(dataRange.getData(),
                  (sumBuffer, dataBuffer) ->
                      checkBytes(sumBuffer, checksumRange.getOffset(),
                          dataBuffer, dataRange.getOffset(), bytesPerSum, file));
          // Now, slice the read data range to the user's ranges
          for(FileRange original: ((CombinedFileRange) dataRange).getUnderlying()) {
            original.setData(result.thenApply(
                (b) -> VectoredReadUtils.sliceTo(b, dataRange.getOffset(), original)));
          }
        }
      }
    }

    @Override
    public boolean hasCapability(String capability) {
      return datas.hasCapability(capability);
    }
  }

  private static class FSDataBoundedInputStream extends FSDataInputStream {
    private FileSystem fs;
    private Path file;
    private long fileLen = -1L;

    FSDataBoundedInputStream(FileSystem fs, Path file, InputStream in) {
      super(in);
      this.fs = fs;
      this.file = file;
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    /* Return the file length */
    private long getFileLength() throws IOException {
      if( fileLen==-1L ) {
        fileLen = fs.getContentSummary(file).getLength();
      }
      return fileLen;
    }

    /**
     * Skips over and discards <code>n</code> bytes of data from the
     * input stream.
     *
     *The <code>skip</code> method skips over some smaller number of bytes
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
      long curPos = getPos();
      long fileLength = getFileLength();
      if( n+curPos > fileLength ) {
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
      if (pos > getFileLength()) {
        throw new EOFException("Cannot seek after EOF");
      }
      super.seek(pos);
    }

  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    FileSystem fs;
    InputStream in;
    if (verifyChecksum) {
      fs = this;
      in = new ChecksumFSInputChecker(this, f, bufferSize);
    } else {
      fs = getRawFileSystem();
      in = fs.open(f, bufferSize);
    }
    return new FSDataBoundedInputStream(fs, f, in);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Append is not supported "
        + "by ChecksumFileSystem");
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    throw new UnsupportedOperationException("Truncate is not supported "
        + "by ChecksumFileSystem");
  }

  @Override
  public void concat(final Path f, final Path[] psrcs) throws IOException {
    throw new UnsupportedOperationException("Concat is not supported "
        + "by ChecksumFileSystem");
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
    return ((size + bytesPerSum - 1) / bytesPerSum) * FSInputChecker.CHECKSUM_SIZE +
             ChecksumFSInputChecker.HEADER_LENGTH;
  }

  /** This class provides an output stream for a checksummed file.
   * It generates checksums for data. */
  private static class ChecksumFSOutputSummer extends FSOutputSummer
      implements IOStatisticsSource, StreamCapabilities {
    private FSDataOutputStream datas;
    private FSDataOutputStream sums;
    private static final float CHKSUM_AS_FRACTION = 0.01f;
    private boolean isClosed = false;

    ChecksumFSOutputSummer(ChecksumFileSystem fs,
                          Path file,
                          boolean overwrite,
                          int bufferSize,
                          short replication,
                          long blockSize,
                          Progressable progress,
                          FsPermission permission)
      throws IOException {
      super(DataChecksum.newDataChecksum(DataChecksum.Type.CRC32,
          fs.getBytesPerSum()));
      int bytesPerSum = fs.getBytesPerSum();
      this.datas = fs.getRawFileSystem().create(file, permission, overwrite,
                                         bufferSize, replication, blockSize,
                                         progress);
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
      this.sums = fs.getRawFileSystem().create(fs.getChecksumFile(file),
                                               permission, true, sumBufferSize,
                                               replication, blockSize, null);
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

    /**
     * Get the IO Statistics of the nested stream, falling back to
     * null if the stream does not implement the interface
     * {@link IOStatisticsSource}.
     * @return an IOStatistics instance or null
     */
    @Override
    public IOStatistics getIOStatistics() {
      return IOStatisticsSupport.retrieveIOStatistics(datas);
    }

    /**
     * Probe the inner stream for a capability.
     * Syncable operations are rejected before being passed down.
     * @param capability string to query the stream support for.
     * @return true if a capability is known to be supported.
     */
    @Override
    public boolean hasCapability(final String capability) {
      if (isProbeForSyncable(capability)) {
        return false;
      }
      return datas.hasCapability(capability);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return create(f, permission, overwrite, true, bufferSize,
        replication, blockSize, progress);
  }

  private FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, boolean createParent, int bufferSize,
      short replication, long blockSize,
      Progressable progress) throws IOException {
    Path parent = f.getParent();
    if (parent != null) {
      if (!createParent && !exists(parent)) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + parent);
      } else if (!mkdirs(parent)) {
        throw new IOException("Mkdirs failed to create " + parent
            + " (exists=" + exists(parent) + ", cwd=" + getWorkingDirectory()
            + ")");
      }
    }
    final FSDataOutputStream out;
    if (writeChecksum) {
      out = new FSDataOutputStream(
          new ChecksumFSOutputSummer(this, f, overwrite, bufferSize, replication,
              blockSize, progress, permission), null);
    } else {
      out = fs.create(f, permission, overwrite, bufferSize, replication,
          blockSize, progress);
      // remove the checksum file since we aren't writing one
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.delete(checkFile, true);
      }
    }
    return out;
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return create(f, permission, overwrite, false, bufferSize, replication,
        blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(final Path f,
      final FsPermission permission,
      final EnumSet<CreateFlag> flags,
      final int bufferSize,
      final short replication,
      final long blockSize,
      final Progressable progress,
      final Options.ChecksumOpt checksumOpt) throws IOException {
    return create(f, permission, flags.contains(CreateFlag.OVERWRITE),
        bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream createNonRecursive(final Path f,
      final FsPermission permission,
      final EnumSet<CreateFlag> flags,
      final int bufferSize,
      final short replication,
      final long blockSize,
      final Progressable progress) throws IOException {
    return create(f, permission, flags.contains(CreateFlag.OVERWRITE),
        false, bufferSize, replication,
        blockSize, progress);
  }

  abstract class FsOperation {
    boolean run(Path p) throws IOException {
      boolean status = apply(p);
      if (status && !p.isRoot()) {
        Path checkFile = getChecksumFile(p);
        if (fs.exists(checkFile)) {
          apply(checkFile);
        }
      }
      return status;
    }
    abstract boolean apply(Path p) throws IOException;
  }


  @Override
  public void setPermission(Path src, final FsPermission permission)
      throws IOException {
    new FsOperation(){
      @Override
      boolean apply(Path p) throws IOException {
        fs.setPermission(p, permission);
        return true;
      }
    }.run(src);
  }

  @Override
  public void setOwner(Path src, final String username, final String groupname)
      throws IOException {
    new FsOperation(){
      @Override
      boolean apply(Path p) throws IOException {
        fs.setOwner(p, username, groupname);
        return true;
      }
    }.run(src);
  }

  @Override
  public void setAcl(Path src, final List<AclEntry> aclSpec)
      throws IOException {
    new FsOperation(){
      @Override
      boolean apply(Path p) throws IOException {
        fs.setAcl(p, aclSpec);
        return true;
      }
    }.run(src);
  }

  @Override
  public void modifyAclEntries(Path src, final List<AclEntry> aclSpec)
      throws IOException {
    new FsOperation(){
      @Override
      boolean apply(Path p) throws IOException {
        fs.modifyAclEntries(p, aclSpec);
        return true;
      }
    }.run(src);
  }

  @Override
  public void removeAcl(Path src) throws IOException {
    new FsOperation(){
      @Override
      boolean apply(Path p) throws IOException {
        fs.removeAcl(p);
        return true;
      }
    }.run(src);
  }

  @Override
  public void removeAclEntries(Path src, final List<AclEntry> aclSpec)
      throws IOException {
    new FsOperation(){
      @Override
      boolean apply(Path p) throws IOException {
        fs.removeAclEntries(p, aclSpec);
        return true;
      }
    }.run(src);
  }

  @Override
  public void removeDefaultAcl(Path src) throws IOException {
    new FsOperation(){
      @Override
      boolean apply(Path p) throws IOException {
        fs.removeDefaultAcl(p);
        return true;
      }
    }.run(src);
  }

  /**
   * Set replication for an existing file.
   * Implement the abstract <code>setReplication</code> of <code>FileSystem</code>
   * @param src file name
   * @param replication new replication
   * @throws IOException if an I/O error occurs.
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  @Override
  public boolean setReplication(Path src, final short replication)
      throws IOException {
    return new FsOperation(){
      @Override
      boolean apply(Path p) throws IOException {
        return fs.setReplication(p, replication);
      }
    }.run(src);
  }

  /**
   * Rename files/dirs.
   */
  @Override
  @SuppressWarnings("deprecation")
  public boolean rename(Path src, Path dst) throws IOException {
    if (fs.isDirectory(src)) {
      return fs.rename(src, dst);
    } else {
      if (fs.isDirectory(dst)) {
        dst = new Path(dst, src.getName());
      }

      boolean value = fs.rename(src, dst);
      if (!value)
        return false;

      Path srcCheckFile = getChecksumFile(src);
      Path dstCheckFile = getChecksumFile(dst);
      if (fs.exists(srcCheckFile)) { //try to rename checksum
        value = fs.rename(srcCheckFile, dstCheckFile);
      } else if (fs.exists(dstCheckFile)) {
        // no src checksum, so remove dst checksum
        value = fs.delete(dstCheckFile, true);
      }

      return value;
    }
  }

  /**
   * Rename files/dirs.
   * <p></p>
   * If the rename of the inner FS does not fail by raising
   * an exception, any checksum file for the source is copied.
   * If the source had no checksum, any checksum for the destination
   * is deleted, so that there is no inconsistent checksum
   * for the now-renamed file.
   * <p></p>
   * @param source source file/dir
   * @param dest destination file/dir/path
   * @param options options for the renaming of the source file.
   *       These are not used for renaming the checksum, which always uses
   *       {@link Options.Rename#OVERWRITE}.Are
   * @throws IOException
   */
  @Override
  public void rename(final Path source,
      final Path dest,
      final Options.Rename... options)
      throws IOException {

    // invoke rename on the wrapped FS.
    // This will raise an exception on any failure.
    fs.rename(source, dest, options);

    // if the application gets this far, the rename
    // succeeded. Therefore the checksum should
    // be renamed too,
    Path srcCheckFile = getChecksumFile(source);
    Path dstCheckFile = getChecksumFile(dest);
    if (fs.exists(srcCheckFile)) {
      // try to rename checksum
      // the OVERWRITE option is always used; there's no attempt
      // to merge in any options supplied for merging the
      // main file.
      // If new options are added, this decision may need to
      // be revisited.
      fs.rename(srcCheckFile, dstCheckFile, Options.Rename.OVERWRITE);
    } else {
      // no src checksum, so remove dest checksum if present
      fs.delete(dstCheckFile, true);
    }
  }

  /**
   * Implement the delete(Path, boolean) in checksum
   * file system.
   */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException{
    FileStatus fstatus = null;
    try {
      fstatus = fs.getFileStatus(f);
    } catch(FileNotFoundException e) {
      return false;
    }
    if (fstatus.isDirectory()) {
      //this works since the crcs are in the same
      //directories and the files. so we just delete
      //everything in the underlying filesystem
      return fs.delete(f, recursive);
    } else {
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.delete(checkFile, true);
      }
      return fs.delete(f, true);
    }
  }

  final private static PathFilter DEFAULT_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path file) {
      return !isChecksumFile(file);
    }
  };

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f
   *          given path
   * @return the statuses of the files/directories in the given path
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return fs.listStatus(f, DEFAULT_FILTER);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path p)
      throws IOException {
    // Not-using fs#listStatusIterator() since it includes crc files as well
    return new DirListingIterator<>(p);
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f
   *          given path
   * @return the statuses of the files/directories in the given patch
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
  throws IOException {
    return fs.listLocatedStatus(f, DEFAULT_FILTER);
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return fs.mkdirs(f);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), src, this, dst, delSrc, conf);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   */
  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(this, src, getLocal(conf), dst, delSrc, conf);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * If src and dst are directories, the copyCrc parameter
   * determines whether to copy CRC files.
   * @param src src path.
   * @param dst dst path.
   * @param copyCrc copy csc flag.
   * @throws IOException if an I/O error occurs.
   */
  @SuppressWarnings("deprecation")
  public void copyToLocalFile(Path src, Path dst, boolean copyCrc)
    throws IOException {
    if (!fs.isDirectory(src)) { // source is a file
      fs.copyToLocalFile(src, dst);
      FileSystem localFs = getLocal(getConf()).getRawFileSystem();
      if (localFs.isDirectory(dst)) {
        dst = new Path(dst, src.getName());
      }
      dst = getChecksumFile(dst);
      if (localFs.exists(dst)) { //remove old local checksum file
        localFs.delete(dst, true);
      }
      Path checksumFile = getChecksumFile(src);
      if (copyCrc && fs.exists(checksumFile)) { //copy checksum file
        fs.copyToLocalFile(checksumFile, dst);
      }
    } else {
      FileStatus[] srcs = listStatus(src);
      for (FileStatus srcFile : srcs) {
        copyToLocalFile(srcFile.getPath(),
                        new Path(dst, srcFile.getPath().getName()), copyCrc);
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
   * @return if retry is necessary
   */
  public boolean reportChecksumFailure(Path f, FSDataInputStream in,
                                       long inPos, FSDataInputStream sums, long sumsPos) {
    return false;
  }

  /**
   * This is overridden to ensure that this class's
   * {@link #openFileWithOptions}() method is called, and so ultimately
   * its {@link #open(Path, int)}.
   *
   * {@inheritDoc}
   */
  @Override
  public FutureDataInputStreamBuilder openFile(final Path path)
      throws IOException, UnsupportedOperationException {
    return ((FutureDataInputStreamBuilderImpl)
        createDataInputStreamBuilder(this, path)).getThisBuilder();
  }

  /**
   * Open the file as a blocking call to {@link #open(Path, int)}.
   *
   * {@inheritDoc}
   */
  @Override
  protected CompletableFuture<FSDataInputStream> openFileWithOptions(
      final Path path,
      final OpenFileParameters parameters) throws IOException {
    AbstractFSBuilderImpl.rejectUnknownMandatoryKeys(
        parameters.getMandatoryKeys(),
        FS_OPTION_OPENFILE_STANDARD_OPTIONS,
        "for " + path);
    return LambdaUtils.eval(
        new CompletableFuture<>(),
        () -> open(path, parameters.getBufferSize()));
  }

  /**
   * This is overridden to ensure that this class's create() method is
   * ultimately called.
   *
   * {@inheritDoc}
   */
  public FSDataOutputStreamBuilder createFile(Path path) {
    return createDataOutputStreamBuilder(this, path)
        .create().overwrite(true);
  }

  /**
   * This is overridden to ensure that this class's create() method is
   * ultimately called.
   *
   * {@inheritDoc}
   */
  public FSDataOutputStreamBuilder appendFile(Path path) {
    return createDataOutputStreamBuilder(this, path).append();
  }

  /**
   * Disable those operations which the checksummed FS blocks.
   * {@inheritDoc}
   */
  @Override
  public boolean hasPathCapability(final Path path, final String capability)
      throws IOException {
    // query the superclass, which triggers argument validation.
    final Path p = makeQualified(path);
    switch (validatePathCapabilityArgs(p, capability)) {
    case CommonPathCapabilities.FS_APPEND:
    case CommonPathCapabilities.FS_CONCAT:
      return false;
    default:
      return super.hasPathCapability(p, capability);
    }
  }

}
