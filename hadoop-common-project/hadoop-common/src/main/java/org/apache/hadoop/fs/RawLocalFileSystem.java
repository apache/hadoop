/*
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

import org.apache.hadoop.classification.VisibleForTesting;

import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileDescriptor;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.StoreImplementationUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.BufferedIOStatisticsOutputStream;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.fs.VectoredReadUtils.LOG_BYTE_BUFFER_RELEASED;
import static org.apache.hadoop.fs.VectoredReadUtils.sortRangeList;
import static org.apache.hadoop.fs.VectoredReadUtils.validateRangeRequest;
import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_EXCEPTIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_SKIP_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_SKIP_OPERATIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_WRITE_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_WRITE_EXCEPTIONS;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/****************************************************************
 * Implement the FileSystem API for the raw local filesystem.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RawLocalFileSystem extends FileSystem {
  static final URI NAME = URI.create("file:///");
  private Path workingDir;
  private long defaultBlockSize;
  // Temporary workaround for HADOOP-9652.
  private static boolean useDeprecatedFileStatus = true;

  @VisibleForTesting
  public static void useStatIfAvailable() {
    useDeprecatedFileStatus = !Stat.isAvailable();
  }
  
  public RawLocalFileSystem() {
    workingDir = getInitialWorkingDirectory();
  }
  
  private Path makeAbsolute(Path f) {
    if (f.isAbsolute()) {
      return f;
    } else {
      return new Path(workingDir, f);
    }
  }
  
  /**
   * Convert a path to a File.
   *
   * @param path the path.
   * @return file.
   */
  public File pathToFile(Path path) {
    checkPath(path);
    if (!path.isAbsolute()) {
      path = new Path(getWorkingDirectory(), path);
    }
    return new File(path.toUri().getPath());
  }

  @Override
  public URI getUri() { return NAME; }
  
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
    defaultBlockSize = getDefaultBlockSize(new Path(uri));
  }
  
  /*******************************************************
   * For open()'s FSInputStream.
   *******************************************************/
  class LocalFSFileInputStream extends FSInputStream implements
      HasFileDescriptor, IOStatisticsSource, StreamCapabilities {
    private FileInputStream fis;
    private final File name;
    private long position;
    private AsynchronousFileChannel asyncChannel = null;

    /**
     * Minimal set of counters.
     */
    private final IOStatisticsStore ioStatistics = iostatisticsStore()
        .withCounters(
            STREAM_READ_BYTES,
            STREAM_READ_EXCEPTIONS,
            STREAM_READ_SEEK_OPERATIONS,
            STREAM_READ_SKIP_OPERATIONS,
            STREAM_READ_SKIP_BYTES)
        .build();

    /** Reference to the bytes read counter for slightly faster counting. */
    private final AtomicLong bytesRead;

    /**
     * Thread level IOStatistics aggregator to update in close().
     */
    private final IOStatisticsAggregator
        ioStatisticsAggregator;

    public LocalFSFileInputStream(Path f) throws IOException {
      name = pathToFile(f);
      fis = new FileInputStream(name);
      bytesRead = ioStatistics.getCounterReference(
          STREAM_READ_BYTES);
      ioStatisticsAggregator =
          IOStatisticsContext.getCurrentIOStatisticsContext().getAggregator();
    }
    
    @Override
    public void seek(long pos) throws IOException {
      if (pos < 0) {
        throw new EOFException(
          FSExceptionMessages.NEGATIVE_SEEK);
      }
      fis.getChannel().position(pos);
      this.position = pos;
    }
    
    @Override
    public long getPos() throws IOException {
      return this.position;
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
    
    /**
     * Just forward to the fis.
     */
    @Override
    public int available() throws IOException { return fis.available(); }
    @Override
    public boolean markSupported() { return false; }

    @Override
    public void close() throws IOException {
      try {
        fis.close();
        if (asyncChannel != null) {
          asyncChannel.close();
        }
      } finally {
        ioStatisticsAggregator.aggregate(ioStatistics);
      }
    }

    @Override
    public int read() throws IOException {
      try {
        int value = fis.read();
        if (value >= 0) {
          this.position++;
          statistics.incrementBytesRead(1);
          bytesRead.addAndGet(1);
        }
        return value;
      } catch (IOException e) {                 // unexpected exception
        ioStatistics.incrementCounter(STREAM_READ_EXCEPTIONS);
        throw new FSError(e);                   // assume native fs error
      }
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      // parameter check
      validatePositionedReadArgs(position, b, off, len);
      try {
        int value = fis.read(b, off, len);
        if (value > 0) {
          this.position += value;
          statistics.incrementBytesRead(value);
          bytesRead.addAndGet(value);
        }
        return value;
      } catch (IOException e) {                 // unexpected exception
        ioStatistics.incrementCounter(STREAM_READ_EXCEPTIONS);
        throw new FSError(e);                   // assume native fs error
      }
    }
    
    @Override
    public int read(long position, byte[] b, int off, int len)
      throws IOException {
      // parameter check
      validatePositionedReadArgs(position, b, off, len);
      if (len == 0) {
        return 0;
      }

      ByteBuffer bb = ByteBuffer.wrap(b, off, len);
      try {
        int value = fis.getChannel().read(bb, position);
        if (value > 0) {
          statistics.incrementBytesRead(value);
          ioStatistics.incrementCounter(STREAM_READ_BYTES, value);
        }
        return value;
      } catch (IOException e) {
        ioStatistics.incrementCounter(STREAM_READ_EXCEPTIONS);
        throw new FSError(e);
      }
    }
    
    @Override
    public long skip(long n) throws IOException {
      ioStatistics.incrementCounter(STREAM_READ_SKIP_OPERATIONS);
      long value = fis.skip(n);
      if (value > 0) {
        this.position += value;
        ioStatistics.incrementCounter(STREAM_READ_SKIP_BYTES, value);
      }
      return value;
    }

    @Override
    public FileDescriptor getFileDescriptor() throws IOException {
      return fis.getFD();
    }

    @Override
    public boolean hasCapability(String capability) {
      // a bit inefficient, but intended to make it easier to add
      // new capabilities.
      switch (capability.toLowerCase(Locale.ENGLISH)) {
      case StreamCapabilities.IOSTATISTICS:
      case StreamCapabilities.IOSTATISTICS_CONTEXT:
      case StreamCapabilities.VECTOREDIO:
        return true;
      default:
        return false;
      }
    }

    @Override
    public IOStatistics getIOStatistics() {
      return ioStatistics;
    }

    AsynchronousFileChannel getAsyncChannel() throws IOException {
      if (asyncChannel == null) {
        synchronized (this) {
          asyncChannel = AsynchronousFileChannel.open(name.toPath(),
                  StandardOpenOption.READ);
        }
      }
      return asyncChannel;
    }

    @Override
    public void readVectored(List<? extends FileRange> ranges,
                             IntFunction<ByteBuffer> allocate) throws IOException {
      readVectored(ranges, allocate, LOG_BYTE_BUFFER_RELEASED);
    }

    @Override
    public void readVectored(final List<? extends FileRange> ranges,
        final IntFunction<ByteBuffer> allocate,
        final Consumer<ByteBuffer> release) throws IOException {

      // Validate, but do not pass in a file length as it may change.
      List<? extends FileRange> sortedRanges = sortRangeList(ranges);
      // Set up all of the futures, so that we can use them if things fail
      for(FileRange range: sortedRanges) {
        validateRangeRequest(range);
        range.setData(new CompletableFuture<>());
      }
      try {
        AsynchronousFileChannel channel = getAsyncChannel();
        ByteBuffer[] buffers = new ByteBuffer[sortedRanges.size()];
        AsyncHandler asyncHandler = new AsyncHandler(channel, sortedRanges, buffers);
        for(int i = 0; i < sortedRanges.size(); ++i) {
          FileRange range = sortedRanges.get(i);
          buffers[i] = allocate.apply(range.getLength());
          channel.read(buffers[i], range.getOffset(), i, asyncHandler);
        }
      } catch (IOException ioe) {
        LOG.debug("Exception occurred during vectored read ", ioe);
        for(FileRange range: sortedRanges) {
          range.getData().completeExceptionally(ioe);
        }
      }
    }
  }

  /**
   * A CompletionHandler that implements readFully and translates back
   * into the form of CompletionHandler that our users expect.
   */
  static class AsyncHandler implements CompletionHandler<Integer, Integer> {
    private final AsynchronousFileChannel channel;
    private final List<? extends FileRange> ranges;
    private final ByteBuffer[] buffers;

    AsyncHandler(AsynchronousFileChannel channel,
                 List<? extends FileRange> ranges,
                 ByteBuffer[] buffers) {
      this.channel = channel;
      this.ranges = ranges;
      this.buffers = buffers;
    }

    @Override
    public void completed(Integer result, Integer r) {
      FileRange range = ranges.get(r);
      ByteBuffer buffer = buffers[r];
      if (result == -1) {
        failed(new EOFException("Read past End of File"), r);
      } else {
        if (buffer.remaining() > 0) {
          // issue a read for the rest of the buffer
          // QQ: What if this fails? It has the same handler.
          channel.read(buffer, range.getOffset() + buffer.position(), r, this);
        } else {
          // QQ: Why  is this required? I think because we don't want the
          // user to read data beyond limit.
          buffer.flip();
          range.getData().complete(buffer);
        }
      }
    }

    @Override
    public void failed(Throwable exc, Integer r) {
      LOG.debug("Failed while reading range {} ", r, exc);
      ranges.get(r).getData().completeExceptionally(exc);
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    getFileStatus(f);
    return new FSDataInputStream(new BufferedFSInputStream(
        new LocalFSFileInputStream(f), bufferSize));
  }

  @Override
  public FSDataInputStream open(PathHandle fd, int bufferSize)
      throws IOException {
    if (!(fd instanceof LocalFileSystemPathHandle)) {
      fd = new LocalFileSystemPathHandle(fd.bytes());
    }
    LocalFileSystemPathHandle id = (LocalFileSystemPathHandle) fd;
    id.verify(getFileStatus(new Path(id.getPath())));
    return new FSDataInputStream(new BufferedFSInputStream(
        new LocalFSFileInputStream(new Path(id.getPath())), bufferSize));
  }

  /*********************************************************
   * For create()'s FSOutputStream.
   *********************************************************/
  final class LocalFSFileOutputStream extends OutputStream implements
      IOStatisticsSource, StreamCapabilities, Syncable {
    private FileOutputStream fos;

    /**
     * Minimal set of counters.
     */
    private final IOStatisticsStore ioStatistics = iostatisticsStore()
        .withCounters(
            STREAM_WRITE_BYTES,
            STREAM_WRITE_EXCEPTIONS)
        .build();

    /**
     * Thread level IOStatistics aggregator to update in close().
     */
    private final IOStatisticsAggregator
        ioStatisticsAggregator;

    private LocalFSFileOutputStream(Path f, boolean append,
        FsPermission permission) throws IOException {
      File file = pathToFile(f);
      // store the aggregator before attempting any IO.
      ioStatisticsAggregator =
          IOStatisticsContext.getCurrentIOStatisticsContext().getAggregator();

      if (!append && permission == null) {
        permission = FsPermission.getFileDefault();
      }
      if (permission == null) {
        this.fos = new FileOutputStream(file, append);
      } else {
        permission = permission.applyUMask(FsPermission.getUMask(getConf()));
        if (Shell.WINDOWS && NativeIO.isAvailable()) {
          this.fos = NativeIO.Windows.createFileOutputStreamWithMode(file,
              append, permission.toShort());
        } else {
          this.fos = new FileOutputStream(file, append);
          boolean success = false;
          try {
            setPermission(f, permission);
            success = true;
          } finally {
            if (!success) {
              IOUtils.cleanupWithLogger(LOG, this.fos);
            }
          }
        }
      }
    }

    /*
     * Close the fos; update the IOStatisticsContext.
     */
    @Override
    public void close() throws IOException {
      try {
        fos.close();
      } finally {
        ioStatisticsAggregator.aggregate(ioStatistics);
      }
    }

    @Override
    public void flush() throws IOException { fos.flush(); }
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      try {
        fos.write(b, off, len);
        ioStatistics.incrementCounter(STREAM_WRITE_BYTES, len);
      } catch (IOException e) {                // unexpected exception
        ioStatistics.incrementCounter(STREAM_WRITE_EXCEPTIONS);
        throw new FSError(e);                  // assume native fs error
      }
    }
    
    @Override
    public void write(int b) throws IOException {
      try {
        fos.write(b);
        ioStatistics.incrementCounter(STREAM_WRITE_BYTES);
      } catch (IOException e) {              // unexpected exception
        ioStatistics.incrementCounter(STREAM_WRITE_EXCEPTIONS);
        throw new FSError(e);                // assume native fs error
      }
    }

    @Override
    public void hflush() throws IOException {
      flush();
    }

    /**
     * HSync calls sync on fhe file descriptor after a local flush() call.
     * @throws IOException failure
     */
    @Override
    public void hsync() throws IOException {
      flush();
      fos.getFD().sync();
    }

    @Override
    public boolean hasCapability(String capability) {
      // a bit inefficient, but intended to make it easier to add
      // new capabilities.
      switch (capability.toLowerCase(Locale.ENGLISH)) {
      case StreamCapabilities.IOSTATISTICS:
      case StreamCapabilities.IOSTATISTICS_CONTEXT:
        return true;
      default:
        return StoreImplementationUtils.isProbeForSyncable(capability);
      }
    }

    @Override
    public IOStatistics getIOStatistics() {
      return ioStatistics;
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    FileStatus status = getFileStatus(f);
    if (status.isDirectory()) {
      throw new IOException("Cannot append to a diretory (=" + f + " )");
    }
    return new FSDataOutputStream(new BufferedOutputStream(
        createOutputStreamWithMode(f, true, null), bufferSize), statistics,
        status.getLen());
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
    short replication, long blockSize, Progressable progress)
    throws IOException {
    return create(f, overwrite, true, bufferSize, replication, blockSize,
        progress, null);
  }

  private FSDataOutputStream create(Path f, boolean overwrite,
      boolean createParent, int bufferSize, short replication, long blockSize,
      Progressable progress, FsPermission permission) throws IOException {
    if (exists(f) && !overwrite) {
      throw new FileAlreadyExistsException("File already exists: " + f);
    }
    Path parent = f.getParent();
    if (parent != null && !mkdirs(parent)) {
      throw new IOException("Mkdirs failed to create " + parent.toString());
    }
    return new FSDataOutputStream(new BufferedIOStatisticsOutputStream(
        createOutputStreamWithMode(f, false, permission), bufferSize, true),
        statistics);
  }
  
  protected OutputStream createOutputStream(Path f, boolean append) 
      throws IOException {
    return createOutputStreamWithMode(f, append, null);
  }

  protected OutputStream createOutputStreamWithMode(Path f, boolean append,
      FsPermission permission) throws IOException {
    return new LocalFSFileOutputStream(f, append, permission);
  }
  
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    if (exists(f) && !flags.contains(CreateFlag.OVERWRITE)) {
      throw new FileAlreadyExistsException("File already exists: " + f);
    }
    return new FSDataOutputStream(new BufferedIOStatisticsOutputStream(
        createOutputStreamWithMode(f, false, permission), bufferSize, true),
            statistics);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
    boolean overwrite, int bufferSize, short replication, long blockSize,
    Progressable progress) throws IOException {

    FSDataOutputStream out = create(f, overwrite, true, bufferSize, replication,
        blockSize, progress, permission);
    return out;
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    FSDataOutputStream out = create(f, overwrite, false, bufferSize, replication,
        blockSize, progress, permission);
    return out;
  }

  @Override
  public void concat(final Path trg, final Path [] psrcs) throws IOException {
    final int bufferSize = 4096;
    try(FSDataOutputStream out = create(trg)) {
      for (Path src : psrcs) {
        try(FSDataInputStream in = open(src)) {
          IOUtils.copyBytes(in, out, bufferSize, false);
        }
      }
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // Attempt rename using Java API.
    File srcFile = pathToFile(src);
    File dstFile = pathToFile(dst);
    if (srcFile.renameTo(dstFile)) {
      return true;
    }

    // Else try POSIX style rename on Windows only
    if (Shell.WINDOWS &&
        handleEmptyDstDirectoryOnWindows(src, srcFile, dst, dstFile)) {
      return true;
    }

    // The fallback behavior accomplishes the rename by a full copy.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Falling through to a copy of " + src + " to " + dst);
    }
    return FileUtil.copy(this, src, this, dst, true, getConf());
  }

  @VisibleForTesting
  public final boolean handleEmptyDstDirectoryOnWindows(Path src, File srcFile,
      Path dst, File dstFile) throws IOException {

    // Enforce POSIX rename behavior that a source directory replaces an
    // existing destination if the destination is an empty directory. On most
    // platforms, this is already handled by the Java API call above. Some
    // platforms (notably Windows) do not provide this behavior, so the Java API
    // call renameTo(dstFile) fails. Delete destination and attempt rename
    // again.
    try {
      FileStatus sdst = this.getFileStatus(dst);
      String[] dstFileList = dstFile.list();
      if (dstFileList != null) {
        if (sdst.isDirectory() && dstFileList.length == 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting empty destination and renaming " + src +
                " to " + dst);
          }
          if (this.delete(dst, false) && srcFile.renameTo(dstFile)) {
            return true;
          }
        }
      }
    } catch (FileNotFoundException ignored) {
    }
    return false;
  }

  @Override
  public boolean truncate(Path f, final long newLength) throws IOException {
    FileStatus status = getFileStatus(f);
    if(status == null) {
      throw new FileNotFoundException("File " + f + " not found");
    }
    if(status.isDirectory()) {
      throw new IOException("Cannot truncate a directory (=" + f + ")");
    }
    long oldLength = status.getLen();
    if(newLength > oldLength) {
      throw new IllegalArgumentException(
          "Cannot truncate to a larger file size. Current size: " + oldLength +
          ", truncate size: " + newLength + ".");
    }
    try (FileOutputStream out = new FileOutputStream(pathToFile(f), true)) {
      try {
        out.getChannel().truncate(newLength);
      } catch(IOException e) {
        throw new FSError(e);
      }
    }
    return true;
  }
  
  /**
   * Delete the given path to a file or directory.
   * @param p the path to delete
   * @param recursive to delete sub-directories
   * @return true if the file or directory and all its contents were deleted
   * @throws IOException if p is non-empty and recursive is false 
   */
  @Override
  public boolean delete(Path p, boolean recursive) throws IOException {
    File f = pathToFile(p);
    if (!f.exists()) {
      //no path, return false "nothing to delete"
      return false;
    }
    if (f.isFile()) {
      return f.delete();
    } else if (!recursive && f.isDirectory() && 
        (FileUtil.listFiles(f).length != 0)) {
      throw new IOException("Directory " + f.toString() + " is not empty");
    }
    return FileUtil.fullyDelete(f);
  }
 
  /**
   * {@inheritDoc}
   *
   * (<b>Note</b>: Returned list is not sorted in any given order,
   * due to reliance on Java's {@link File#list()} API.)
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    File localf = pathToFile(f);
    FileStatus[] results;

    if (!localf.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist");
    }

    if (localf.isDirectory()) {
      String[] names = FileUtil.list(localf);
      results = new FileStatus[names.length];
      int j = 0;
      for (int i = 0; i < names.length; i++) {
        try {
          // Assemble the path using the Path 3 arg constructor to make sure
          // paths with colon are properly resolved on Linux
          results[j] = getFileStatus(new Path(f, new Path(null, null,
                                                          names[i])));
          j++;
        } catch (FileNotFoundException e) {
          // ignore the files not found since the dir list may have have
          // changed since the names[] list was generated.
        }
      }
      if (j == names.length) {
        return results;
      }
      return Arrays.copyOf(results, j);
    }

    if (!useDeprecatedFileStatus) {
      return new FileStatus[] { getFileStatus(f) };
    }
    return new FileStatus[] {
        new DeprecatedRawLocalFileStatus(localf,
        defaultBlockSize, this) };
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return pathToFile(f).exists();
  }
  
  protected boolean mkOneDir(File p2f) throws IOException {
    return mkOneDirWithMode(new Path(p2f.getAbsolutePath()), p2f, null);
  }

  protected boolean mkOneDirWithMode(Path p, File p2f, FsPermission permission)
      throws IOException {
    if (permission == null) {
      permission = FsPermission.getDirDefault();
    }
    permission = permission.applyUMask(FsPermission.getUMask(getConf()));
    if (Shell.WINDOWS && NativeIO.isAvailable()) {
      try {
        NativeIO.Windows.createDirectoryWithMode(p2f, permission.toShort());
        return true;
      } catch (IOException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format(
              "NativeIO.createDirectoryWithMode error, path = %s, mode = %o",
              p2f, permission.toShort()), e);
        }
        return false;
      }
    } else {
      boolean b = p2f.mkdir();
      if (b) {
        setPermission(p, permission);
      }
      return b;
    }
  }

  /**
   * Creates the specified directory hierarchy. Does not
   * treat existence as an error.
   */
  @Override
  public boolean mkdirs(Path f) throws IOException {
    return mkdirsWithOptionalPermission(f, null);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return mkdirsWithOptionalPermission(f, permission);
  }

  private boolean mkdirsWithOptionalPermission(Path f, FsPermission permission)
      throws IOException {
    if(f == null) {
      throw new IllegalArgumentException("mkdirs path arg is null");
    }
    Path parent = f.getParent();
    File p2f = pathToFile(f);
    File parent2f = null;
    if(parent != null) {
      parent2f = pathToFile(parent);
      if(parent2f != null && parent2f.exists() && !parent2f.isDirectory()) {
        throw new ParentNotDirectoryException("Parent path is not a directory: "
            + parent);
      }
    }
    if (p2f.exists() && !p2f.isDirectory()) {
      throw new FileAlreadyExistsException("Destination exists" +
              " and is not a directory: " + p2f.getCanonicalPath());
    }
    return (parent == null || parent2f.exists() || mkdirs(parent)) &&
      (mkOneDirWithMode(f, p2f, permission) || p2f.isDirectory());
  }
  
  
  @Override
  public Path getHomeDirectory() {
    return this.makeQualified(new Path(System.getProperty("user.home")));
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = makeAbsolute(newDir);
    checkPath(workingDir);
  }
  
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }
  
  @Override
  protected Path getInitialWorkingDirectory() {
    return this.makeQualified(new Path(System.getProperty("user.dir")));
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    File partition = pathToFile(p == null ? new Path("/") : p);
    //File provides getUsableSpace() and getFreeSpace()
    //File provides no API to obtain used space, assume used = total - free
    return new FsStatus(partition.getTotalSpace(), 
      partition.getTotalSpace() - partition.getFreeSpace(),
      partition.getFreeSpace());
  }
  
  // In the case of the local filesystem, we can just rename the file.
  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    rename(src, dst);
  }
  
  // We can write output directly to the final location
  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return fsOutputFile;
  }
  
  // It's in the right place - nothing to do.
  @Override
  public void completeLocalOutput(Path fsWorkingFile, Path tmpLocalFile)
    throws IOException {
  }
  
  @Override
  public void close() throws IOException {
    super.close();
  }
  
  @Override
  public String toString() {
    return "LocalFS";
  }
  
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return getFileLinkStatusInternal(f, true);
  }

  @Deprecated
  private FileStatus deprecatedGetFileStatus(Path f) throws IOException {
    File path = pathToFile(f);
    if (path.exists()) {
      return new DeprecatedRawLocalFileStatus(pathToFile(f),
          defaultBlockSize, this);
    } else {
      throw new FileNotFoundException("File " + f + " does not exist");
    }
  }

  @Deprecated
  static class DeprecatedRawLocalFileStatus extends FileStatus {
    /* We can add extra fields here. It breaks at least CopyFiles.FilePair().
     * We recognize if the information is already loaded by check if
     * onwer.equals("").
     */
    private boolean isPermissionLoaded() {
      return !super.getOwner().isEmpty(); 
    }

    private static long getLastAccessTime(File f) throws IOException {
      long accessTime;
      try {
        accessTime = Files.readAttributes(f.toPath(),
            BasicFileAttributes.class).lastAccessTime().toMillis();
      } catch (NoSuchFileException e) {
        throw new FileNotFoundException("File " + f + " does not exist");
      }
      return accessTime;
    }

    DeprecatedRawLocalFileStatus(File f, long defaultBlockSize, FileSystem fs)
      throws IOException {
      super(f.length(), f.isDirectory(), 1, defaultBlockSize,
          f.lastModified(), getLastAccessTime(f),
          null, null, null,
          new Path(f.getPath()).makeQualified(fs.getUri(),
            fs.getWorkingDirectory()));
    }
    
    @Override
    public FsPermission getPermission() {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getPermission();
    }

    @Override
    public String getOwner() {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getOwner();
    }

    @Override
    public String getGroup() {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getGroup();
    }

    /**
     * Load file permission information (UNIX symbol rwxrwxrwx, sticky bit info).
     *
     * To improve peformance, give priority to native stat() call. First try get
     * permission information by using native JNI call then fall back to use non
     * native (ProcessBuilder) call in case native lib is not loaded or native
     * call is not successful
     */
    private synchronized void loadPermissionInfo() {
      if (!isPermissionLoaded() && NativeIO.isAvailable()) {
        try {
          loadPermissionInfoByNativeIO();
        } catch (IOException ex) {
          LOG.debug("Native call failed", ex);
        }
      }

      if (!isPermissionLoaded()) {
        loadPermissionInfoByNonNativeIO();
      }
    }

    /// loads permissions, owner, and group from `ls -ld`
    @VisibleForTesting
    void loadPermissionInfoByNonNativeIO() {
      IOException e = null;
      try {
        String output = FileUtil.execCommand(new File(getPath().toUri()),
            Shell.getGetPermissionCommand());
        StringTokenizer t =
            new StringTokenizer(output, Shell.TOKEN_SEPARATOR_REGEX);
        //expected format
        //-rw-------    1 username groupname ...
        String permission = t.nextToken();
        if (permission.length() > FsPermission.MAX_PERMISSION_LENGTH) {
          //files with ACLs might have a '+'
          permission = permission.substring(0,
            FsPermission.MAX_PERMISSION_LENGTH);
        }
        setPermission(FsPermission.valueOf(permission));
        t.nextToken();

        String owner = t.nextToken();
        String group = t.nextToken();
        // If on windows domain, token format is DOMAIN\\user and we want to
        // extract only the user name
        // same as to the group name
        if (Shell.WINDOWS) {
          owner = removeDomain(owner);
          group = removeDomain(group);
        }
        setOwner(owner);
        setGroup(group);
      } catch (Shell.ExitCodeException ioe) {
        if (ioe.getExitCode() != 1) {
          e = ioe;
        } else {
          setPermission(null);
          setOwner(null);
          setGroup(null);
        }
      } catch (IOException ioe) {
        e = ioe;
      } finally {
        if (e != null) {
          throw new RuntimeException("Error while running command to get " +
                                     "file permissions : " + 
                                     StringUtils.stringifyException(e));
        }
      }
    }

    // In Windows, domain name is added.
    // For example, given machine name (domain name) dname, user name i, then
    // the result for user is dname\\i and for group is dname\\None. So we need
    // remove domain name as follows:
    // DOMAIN\\user => user, DOMAIN\\group => group
    private String removeDomain(String str) {
      int index = str.indexOf("\\");
      if (index != -1) {
        str = str.substring(index + 1);
      }
      return str;
    }

    // loads permissions, owner, and group from `ls -ld`
    // but use JNI to more efficiently get file mode (permission, owner, group)
    // by calling file stat() in *nix or some similar calls in Windows
    @VisibleForTesting
    void loadPermissionInfoByNativeIO() throws IOException {
      Path path = getPath();
      String pathName = path.toUri().getPath();
      // remove leading slash for Windows path
      if (Shell.WINDOWS && pathName.startsWith("/")) {
        pathName = pathName.substring(1);
      }
      try {
        NativeIO.POSIX.Stat stat = NativeIO.POSIX.getStat(pathName);
        String owner = stat.getOwner();
        String group = stat.getGroup();
        int mode = stat.getMode();
        setOwner(owner);
        setGroup(group);
        setPermission(new FsPermission(mode));
      } catch (IOException e) {
        setOwner(null);
        setGroup(null);
        setPermission(null);
        throw e;
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      super.write(out);
    }
  }

  /**
   * Use the command chown to set owner.
   */
  @Override
  public void setOwner(Path p, String username, String groupname)
    throws IOException {
    FileUtil.setOwner(pathToFile(p), username, groupname);
  }

  /**
   * Use the command chmod to set permission.
   */
  @Override
  public void setPermission(Path p, FsPermission permission)
    throws IOException {
    if (NativeIO.isAvailable()) {
      NativeIO.POSIX.chmod(pathToFile(p).getCanonicalPath(),
                     permission.toShort());
    } else {
      String perm = String.format("%04o", permission.toShort());
      Shell.execCommand(Shell.getSetPermissionCommand(perm, false,
        FileUtil.makeShellPath(pathToFile(p), true)));
    }
  }
 
  /**
   * Sets the {@link Path}'s last modified time and last access time to
   * the given valid times.
   *
   * @param mtime the modification time to set (only if no less than zero).
   * @param atime the access time to set (only if no less than zero).
   * @throws IOException if setting the times fails.
   */
  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    try {
      BasicFileAttributeView view = Files.getFileAttributeView(
          pathToFile(p).toPath(), BasicFileAttributeView.class);
      FileTime fmtime = (mtime >= 0) ? FileTime.fromMillis(mtime) : null;
      FileTime fatime = (atime >= 0) ? FileTime.fromMillis(atime) : null;
      view.setTimes(fmtime, fatime, null);
    } catch (NoSuchFileException e) {
      throw new FileNotFoundException("File " + p + " does not exist");
    }
  }

  /**
   * Hook to implement support for {@link PathHandle} operations.
   * @param stat Referent in the target FileSystem
   * @param opts Constraints that determine the validity of the
   *            {@link PathHandle} reference.
   */
  protected PathHandle createPathHandle(FileStatus stat,
      Options.HandleOpt... opts) {
    if (stat.isDirectory() || stat.isSymlink()) {
      throw new IllegalArgumentException("PathHandle only available for files");
    }
    String authority = stat.getPath().toUri().getAuthority();
    if (authority != null && !authority.equals("file://")) {
      throw new IllegalArgumentException("Wrong FileSystem: " + stat.getPath());
    }
    Options.HandleOpt.Data data =
        Options.HandleOpt.getOpt(Options.HandleOpt.Data.class, opts)
            .orElse(Options.HandleOpt.changed(false));
    Options.HandleOpt.Location loc =
        Options.HandleOpt.getOpt(Options.HandleOpt.Location.class, opts)
            .orElse(Options.HandleOpt.moved(false));
    if (loc.allowChange()) {
      throw new UnsupportedOperationException("Tracking file movement in " +
          "basic FileSystem is not supported");
    }
    final Path p = stat.getPath();
    final Optional<Long> mtime = !data.allowChange()
        ? Optional.of(stat.getModificationTime())
        : Optional.empty();
    return new LocalFileSystemPathHandle(p.toString(), mtime);
  }

  @Override
  public boolean supportsSymlinks() {
    return true;
  }

  @SuppressWarnings("deprecation")
  @Override
  public void createSymlink(Path target, Path link, boolean createParent)
      throws IOException {
    if (!FileSystem.areSymlinksEnabled()) {
      throw new UnsupportedOperationException("Symlinks not supported");
    }
    final String targetScheme = target.toUri().getScheme();
    if (targetScheme != null && !"file".equals(targetScheme)) {
      throw new IOException("Unable to create symlink to non-local file "+
                            "system: "+target.toString());
    }
    if (createParent) {
      mkdirs(link.getParent());
    }

    // NB: Use createSymbolicLink in java.nio.file.Path once available
    int result = FileUtil.symLink(target.toString(),
        makeAbsolute(link).toString());
    if (result != 0) {
      throw new IOException("Error " + result + " creating symlink " +
          link + " to " + target);
    }
  }

  /**
   * Return a FileStatus representing the given path. If the path refers
   * to a symlink return a FileStatus representing the link rather than
   * the object the link refers to.
   */
  @Override
  public FileStatus getFileLinkStatus(final Path f) throws IOException {
    FileStatus fi = getFileLinkStatusInternal(f, false);
    // getFileLinkStatus is supposed to return a symlink with a
    // qualified path
    if (fi.isSymlink()) {
      Path targetQual = FSLinkResolver.qualifySymlinkTarget(this.getUri(),
          fi.getPath(), fi.getSymlink());
      fi.setSymlink(targetQual);
    }
    return fi;
  }

  /**
   * Public {@link FileStatus} methods delegate to this function, which in turn
   * either call the new {@link Stat} based implementation or the deprecated
   * methods based on platform support.
   * 
   * @param f Path to stat
   * @param dereference whether to dereference the final path component if a
   *          symlink
   * @return FileStatus of f
   * @throws IOException
   */
  private FileStatus getFileLinkStatusInternal(final Path f,
      boolean dereference) throws IOException {
    if (!useDeprecatedFileStatus) {
      return getNativeFileLinkStatus(f, dereference);
    } else if (dereference) {
      return deprecatedGetFileStatus(f);
    } else {
      return deprecatedGetFileLinkStatusInternal(f);
    }
  }

  /**
   * Deprecated. Remains for legacy support. Should be removed when {@link Stat}
   * gains support for Windows and other operating systems.
   */
  @Deprecated
  private FileStatus deprecatedGetFileLinkStatusInternal(final Path f)
      throws IOException {
    String target = FileUtil.readLink(new File(f.toString()));

    try {
      FileStatus fs = getFileStatus(f);
      // If f refers to a regular file or directory
      if (target.isEmpty()) {
        return fs;
      }
      // Otherwise f refers to a symlink
      return new FileStatus(fs.getLen(),
          false,
          fs.getReplication(),
          fs.getBlockSize(),
          fs.getModificationTime(),
          fs.getAccessTime(),
          fs.getPermission(),
          fs.getOwner(),
          fs.getGroup(),
          new Path(target),
          f);
    } catch (FileNotFoundException e) {
      /* The exists method in the File class returns false for dangling
       * links so we can get a FileNotFoundException for links that exist.
       * It's also possible that we raced with a delete of the link. Use
       * the readBasicFileAttributes method in java.nio.file.attributes
       * when available.
       */
      if (!target.isEmpty()) {
        return new FileStatus(0, false, 0, 0, 0, 0, FsPermission.getDefault(),
            "", "", new Path(target), f);
      }
      // f refers to a file or directory that does not exist
      throw e;
    }
  }
  /**
   * Calls out to platform's native stat(1) implementation to get file metadata
   * (permissions, user, group, atime, mtime, etc). This works around the lack
   * of lstat(2) in Java 6.
   * 
   *  Currently, the {@link Stat} class used to do this only supports Linux
   *  and FreeBSD, so the old {@link #deprecatedGetFileLinkStatusInternal(Path)}
   *  implementation (deprecated) remains further OS support is added.
   *
   * @param f File to stat
   * @param dereference whether to dereference symlinks
   * @return FileStatus of f
   * @throws IOException
   */
  private FileStatus getNativeFileLinkStatus(final Path f,
      boolean dereference) throws IOException {
    checkPath(f);
    Stat stat = new Stat(f, defaultBlockSize, dereference, this);
    FileStatus status = stat.getFileStatus();
    return status;
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    FileStatus fi = getFileLinkStatusInternal(f, false);
    // return an unqualified symlink target
    return fi.getSymlink();
  }

  @Override
  public boolean hasPathCapability(final Path path, final String capability)
      throws IOException {
    switch (validatePathCapabilityArgs(makeQualified(path), capability)) {
    case CommonPathCapabilities.FS_APPEND:
    case CommonPathCapabilities.FS_CONCAT:
    case CommonPathCapabilities.FS_PATHHANDLES:
    case CommonPathCapabilities.FS_PERMISSIONS:
    case CommonPathCapabilities.FS_TRUNCATE:
      // block locations are generated locally
    case CommonPathCapabilities.VIRTUAL_BLOCK_LOCATIONS:
      return true;
    case CommonPathCapabilities.FS_SYMLINKS:
      return FileSystem.areSymlinksEnabled();
    default:
      return super.hasPathCapability(path, capability);
    }
  }

  @VisibleForTesting
  static void setUseDeprecatedFileStatus(boolean useDeprecatedFileStatus) {
    RawLocalFileSystem.useDeprecatedFileStatus = useDeprecatedFileStatus;
  }
}
