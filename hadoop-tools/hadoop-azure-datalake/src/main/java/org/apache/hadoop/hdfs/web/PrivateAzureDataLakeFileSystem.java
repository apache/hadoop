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
 *
 */

package org.apache.hadoop.hdfs.web;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.web.resources.ADLFlush;
import org.apache.hadoop.hdfs.web.resources.ADLGetOpParam;
import org.apache.hadoop.hdfs.web.resources.ADLPostOpParam;
import org.apache.hadoop.hdfs.web.resources.ADLPutOpParam;
import org.apache.hadoop.hdfs.web.resources.ADLVersionInfo;
import org.apache.hadoop.hdfs.web.resources.AppendADLNoRedirectParam;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.CreateADLNoRedirectParam;
import org.apache.hadoop.hdfs.web.resources.CreateFlagParam;
import org.apache.hadoop.hdfs.web.resources.CreateParentParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LeaseParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.ReadADLNoRedirectParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.VersionInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Extended @see SWebHdfsFileSystem API. This class contains Azure data lake
 * specific stability, Reliability and performance improvement.
 * <p>
 * Motivation behind PrivateAzureDataLakeFileSystem to encapsulate dependent
 * implementation on org.apache.hadoop.hdfs.web package to configure query
 * parameters, configuration over HTTP request send to backend .. etc. This
 * class should be refactored and moved under package org.apache.hadoop.fs
 * .adl once the required dependent changes are made into ASF code.
 */
public class PrivateAzureDataLakeFileSystem extends SWebHdfsFileSystem {

  public static final String SCHEME = "adl";

  // Feature configuration
  private boolean featureGetBlockLocationLocallyBundled = true;
  private boolean featureConcurrentReadWithReadAhead = true;
  private boolean featureRedirectOff = true;
  private boolean featureFlushWhenEOF = true;
  private boolean overrideOwner = false;
  private int maxConcurrentConnection;
  private int maxBufferSize;
  private String userName;

  /**
   * Constructor.
   */
  public PrivateAzureDataLakeFileSystem() {
    try {
      userName = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      userName = "hadoop";
    }
  }

  @Override
  public synchronized void initialize(URI uri, Configuration conf)
      throws IOException {
    super.initialize(uri, conf);
    overrideOwner = getConf()
        .getBoolean(ADLConfKeys.ADL_DEBUG_OVERRIDE_LOCAL_USER_AS_OWNER,
            ADLConfKeys.ADL_DEBUG_SET_LOCAL_USER_AS_OWNER_DEFAULT);

    featureRedirectOff = getConf()
        .getBoolean(ADLConfKeys.ADL_FEATURE_REDIRECT_OFF,
            ADLConfKeys.ADL_FEATURE_REDIRECT_OFF_DEFAULT);

    featureGetBlockLocationLocallyBundled = getConf()
        .getBoolean(ADLConfKeys.ADL_FEATURE_GET_BLOCK_LOCATION_LOCALLY_BUNDLED,
            ADLConfKeys.ADL_FEATURE_GET_BLOCK_LOCATION_LOCALLY_BUNDLED_DEFAULT);

    featureConcurrentReadWithReadAhead = getConf().
        getBoolean(ADLConfKeys.ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD,
            ADLConfKeys.ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_DEFAULT);

    maxBufferSize = getConf().getInt(
        ADLConfKeys.ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_BUFFER_SIZE,
        ADLConfKeys
            .ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_BUFFER_SIZE_DEFAULT);

    maxConcurrentConnection = getConf().getInt(
        ADLConfKeys.ADL_FEATURE_CONCURRENT_READ_AHEAD_MAX_CONCURRENT_CONN,
        ADLConfKeys
            .ADL_FEATURE_CONCURRENT_READ_AHEAD_MAX_CONCURRENT_CONN_DEFAULT);
  }

  @VisibleForTesting
  protected boolean isFeatureGetBlockLocationLocallyBundled() {
    return featureGetBlockLocationLocallyBundled;
  }

  @VisibleForTesting
  protected boolean isFeatureConcurrentReadWithReadAhead() {
    return featureConcurrentReadWithReadAhead;
  }

  @VisibleForTesting
  protected boolean isFeatureRedirectOff() {
    return featureRedirectOff;
  }

  @VisibleForTesting
  protected boolean isOverrideOwnerFeatureOn() {
    return overrideOwner;
  }

  @VisibleForTesting
  protected int getMaxBufferSize() {
    return maxBufferSize;
  }

  @VisibleForTesting
  protected int getMaxConcurrentConnection() {
    return maxConcurrentConnection;
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  /**
   * Constructing home directory locally is fine as long as Hadoop
   * local user name and ADL user name relationship story is not fully baked
   * yet.
   *
   * @return Hadoop local user home directory.
   */
  @Override
  public final Path getHomeDirectory() {
    try {
      return makeQualified(new Path(
          "/user/" + UserGroupInformation.getCurrentUser().getShortUserName()));
    } catch (IOException e) {
    }

    return new Path("/user/" + userName);
  }

  /**
   * Azure data lake does not support user configuration for data replication
   * hence not leaving system to query on
   * azure data lake.
   *
   * Stub implementation
   *
   * @param p           Not honoured
   * @param replication Not honoured
   * @return True hard coded since ADL file system does not support
   * replication configuration
   * @throws IOException No exception would not thrown in this case however
   *                     aligning with parent api definition.
   */
  @Override
  public final boolean setReplication(final Path p, final short replication)
      throws IOException {
    return true;
  }

  /**
   * @param f File/Folder path
   * @return FileStatus instance containing metadata information of f
   * @throws IOException For any system error
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    statistics.incrementReadOps(1);
    FileStatus status = super.getFileStatus(f);

    if (overrideOwner) {
      FileStatus proxiedStatus = new FileStatus(status.getLen(),
          status.isDirectory(), status.getReplication(), status.getBlockSize(),
          status.getModificationTime(), status.getAccessTime(),
          status.getPermission(), userName, "hdfs", status.getPath());
      return proxiedStatus;
    } else {
      return status;
    }
  }

  /**
   * Create call semantic is handled differently in case of ADL. Create
   * semantics is translated to Create/Append
   * semantics.
   * 1. No dedicated connection to server.
   * 2. Buffering is locally done, Once buffer is full or flush is invoked on
   * the by the caller. All the pending
   * data is pushed to ADL as APPEND operation code.
   * 3. On close - Additional call is send to server to close the stream, and
   * release lock from the stream.
   *
   * Necessity of Create/Append semantics is
   * 1. ADL backend server does not allow idle connection for longer duration
   * . In case of slow writer scenario,
   * observed connection timeout/Connection reset causing occasional job
   * failures.
   * 2. Performance boost to jobs which are slow writer, avoided network latency
   * 3. ADL equally better performing with multiple of 4MB chunk as append
   * calls.
   *
   * @param f           File path
   * @param permission  Access permission for the newly created file
   * @param overwrite   Remove existing file and recreate new one if true
   *                    otherwise throw error if file exist
   * @param bufferSize  Buffer size, ADL backend does not honour
   * @param replication Replication count, ADL backend does not honour
   * @param blockSize   Block size, ADL backend does not honour
   * @param progress    Progress indicator
   * @return FSDataOutputStream OutputStream on which application can push
   * stream of bytes
   * @throws IOException when system error, internal server error or user error
   */
  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);

    return new FSDataOutputStream(new BatchAppendOutputStream(f, bufferSize,
        new PermissionParam(applyUMask(permission)),
        new OverwriteParam(overwrite), new BufferSizeParam(bufferSize),
        new ReplicationParam(replication), new BlockSizeParam(blockSize),
        new ADLVersionInfo(VersionInfo.getVersion())), statistics) {
    };
  }

  @Override
  public FSDataOutputStream createNonRecursive(final Path f,
      final FsPermission permission, final EnumSet<CreateFlag> flag,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);

    String leaseId = java.util.UUID.randomUUID().toString();
    return new FSDataOutputStream(new BatchAppendOutputStream(f, bufferSize,
        new PermissionParam(applyUMask(permission)), new CreateFlagParam(flag),
        new CreateParentParam(false), new BufferSizeParam(bufferSize),
        new ReplicationParam(replication), new LeaseParam(leaseId),
        new BlockSizeParam(blockSize),
        new ADLVersionInfo(VersionInfo.getVersion())), statistics) {
    };
  }

  /**
   * Since defined as private in parent class, redefined to pass through
   * Create api implementation.
   *
   * @param permission
   * @return FsPermission list
   */
  private FsPermission applyUMask(FsPermission permission) {
    FsPermission fsPermission = permission;
    if (fsPermission == null) {
      fsPermission = FsPermission.getDefault();
    }
    return fsPermission.applyUMask(FsPermission.getUMask(getConf()));
  }

  /**
   * Open call semantic is handled differently in case of ADL. Instead of
   * network stream is returned to the user,
   * Overridden FsInputStream is returned.
   *
   * 1. No dedicated connection to server.
   * 2. Process level concurrent read ahead Buffering is done, This allows
   * data to be available for caller quickly.
   * 3. Number of byte to read ahead is configurable.
   *
   * Advantage of Process level concurrent read ahead Buffering semantics is
   * 1. ADL backend server does not allow idle connection for longer duration
   * . In case of slow reader scenario,
   * observed connection timeout/Connection reset causing occasional job
   * failures.
   * 2. Performance boost to jobs which are slow reader, avoided network latency
   * 3. Compressed format support like ORC, and large data files gains the
   * most out of this implementation.
   *
   * Read ahead feature is configurable.
   *
   * @param f          File path
   * @param buffersize Buffer size
   * @return FSDataInputStream InputStream on which application can read
   * stream of bytes
   * @throws IOException when system error, internal server error or user error
   */
  @Override
  public FSDataInputStream open(final Path f, final int buffersize)
      throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.OPEN;
    // use a runner so the open can recover from an invalid token
    FsPathConnectionRunner runner = null;

    if (featureConcurrentReadWithReadAhead) {
      URL url = this.toUrl(op, f, new BufferSizeParam(buffersize),
          new ReadADLNoRedirectParam(true),
          new ADLVersionInfo(VersionInfo.getVersion()));

      BatchByteArrayInputStream bb = new BatchByteArrayInputStream(url, f,
          maxBufferSize, maxConcurrentConnection);

      FSDataInputStream fin = new FSDataInputStream(bb);
      return fin;
    } else {
      if (featureRedirectOff) {
        runner = new FsPathConnectionRunner(ADLGetOpParam.Op.OPEN, f,
            new BufferSizeParam(buffersize), new ReadADLNoRedirectParam(true),
            new ADLVersionInfo(VersionInfo.getVersion()));
      } else {
        runner = new FsPathConnectionRunner(op, f,
            new BufferSizeParam(buffersize));
      }

      return new FSDataInputStream(
          new OffsetUrlInputStream(new UnresolvedUrlOpener(runner),
              new OffsetUrlOpener(null)));
    }
  }

  /**
   * @param f File/Folder path
   * @return FileStatus array list
   * @throws IOException For system error
   */
  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    FileStatus[] fileStatuses = super.listStatus(f);
    for (int i = 0; i < fileStatuses.length; i++) {
      if (overrideOwner) {
        fileStatuses[i] = new FileStatus(fileStatuses[i].getLen(),
            fileStatuses[i].isDirectory(), fileStatuses[i].getReplication(),
            fileStatuses[i].getBlockSize(),
            fileStatuses[i].getModificationTime(),
            fileStatuses[i].getAccessTime(), fileStatuses[i].getPermission(),
            userName, "hdfs", fileStatuses[i].getPath());
      }
    }
    return fileStatuses;
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus status,
      final long offset, final long length) throws IOException {
    if (status == null) {
      return null;
    }

    if (featureGetBlockLocationLocallyBundled) {
      if ((offset < 0) || (length < 0)) {
        throw new IllegalArgumentException("Invalid start or len parameter");
      }

      if (status.getLen() < offset) {
        return new BlockLocation[0];
      }

      final String[] name = {"localhost"};
      final String[] host = {"localhost"};
      long blockSize = ADLConfKeys.DEFAULT_EXTENT_SIZE; // Block size must be
      // non zero
      int numberOfLocations =
          (int) (length / blockSize) + ((length % blockSize == 0) ? 0 : 1);
      BlockLocation[] locations = new BlockLocation[numberOfLocations];
      for (int i = 0; i < locations.length; i++) {
        long currentOffset = offset + (i * blockSize);
        long currentLength = Math
            .min(blockSize, offset + length - currentOffset);
        locations[i] = new BlockLocation(name, host, currentOffset,
            currentLength);
      }

      return locations;
    } else {
      return getFileBlockLocations(status.getPath(), offset, length);
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final Path p, final long offset,
      final long length) throws IOException {
    statistics.incrementReadOps(1);

    if (featureGetBlockLocationLocallyBundled) {
      FileStatus fileStatus = getFileStatus(p);
      return getFileBlockLocations(fileStatus, offset, length);
    } else {
      return super.getFileBlockLocations(p, offset, length);
    }
  }

  enum StreamState {
    Initial,
    DataCachedInLocalBuffer,
    StreamEnd
  }

  class BatchAppendOutputStream extends OutputStream {
    private Path fsPath;
    private Param<?, ?>[] parameters;
    private byte[] data = null;
    private int offset = 0;
    private long length = 0;
    private boolean eof = false;
    private boolean hadError = false;
    private byte[] dataBuffers = null;
    private int bufSize = 0;
    private boolean streamClosed = false;

    public BatchAppendOutputStream(Path path, int bufferSize,
        Param<?, ?>... param) throws IOException {
      if (bufferSize < (ADLConfKeys.DEFAULT_BLOCK_SIZE)) {
        bufSize = ADLConfKeys.DEFAULT_BLOCK_SIZE;
      } else {
        bufSize = bufferSize;
      }

      this.fsPath = path;
      this.parameters = param;
      this.data = getBuffer();
      FSDataOutputStream createStream = null;
      try {
        if (featureRedirectOff) {
          CreateADLNoRedirectParam skipRedirect = new CreateADLNoRedirectParam(
              true);
          Param<?, ?>[] tmpParam = featureFlushWhenEOF ?
              new Param<?, ?>[param.length + 2] :
              new Param<?, ?>[param.length + 1];
          System.arraycopy(param, 0, tmpParam, 0, param.length);
          tmpParam[param.length] = skipRedirect;
          if (featureFlushWhenEOF) {
            tmpParam[param.length + 1] = new ADLFlush(false);
          }
          createStream = new FsPathOutputStreamRunner(ADLPutOpParam.Op.CREATE,
              fsPath, 1, tmpParam).run();
        } else {
          createStream = new FsPathOutputStreamRunner(PutOpParam.Op.CREATE,
              fsPath, 1, param).run();
        }
      } finally {
        if (createStream != null) {
          createStream.close();
        }
      }
    }

    @Override
    public final synchronized void write(int b) throws IOException {
      if (streamClosed) {
        throw new IOException(fsPath + " stream object is closed.");
      }

      if (offset == (data.length)) {
        flush();
      }

      data[offset] = (byte) b;
      offset++;

      // Statistics will get incremented again as part of the batch updates,
      // decrement here to avoid double value
      if (statistics != null) {
        statistics.incrementBytesWritten(-1);
      }
    }

    @Override
    public final synchronized void write(byte[] buf, int off, int len)
        throws IOException {
      if (streamClosed) {
        throw new IOException(fsPath + " stream object is closed.");
      }

      int bytesToWrite = len;
      int localOff = off;
      int localLen = len;
      if (localLen >= data.length) {
        // Flush data that is already in our internal buffer
        flush();

        // Keep committing data until we have less than our internal buffers
        // length left
        do {
          try {
            commit(buf, localOff, data.length, eof);
          } catch (IOException e) {
            hadError = true;
            throw e;
          }
          localOff += data.length;
          localLen -= data.length;
        } while (localLen >= data.length);
      }

      // At this point, we have less than data.length left to copy from users
      // buffer
      if (offset + localLen >= data.length) {
        // Users buffer has enough data left to fill our internal buffer
        int bytesToCopy = data.length - offset;
        System.arraycopy(buf, localOff, data, offset, bytesToCopy);
        offset += bytesToCopy;

        // Flush our internal buffer
        flush();
        localOff += bytesToCopy;
        localLen -= bytesToCopy;
      }

      if (localLen > 0) {
        // Simply copy the remainder from the users buffer into our internal
        // buffer
        System.arraycopy(buf, localOff, data, offset, localLen);
        offset += localLen;
      }

      // Statistics will get incremented again as part of the batch updates,
      // decrement here to avoid double value
      if (statistics != null) {
        statistics.incrementBytesWritten(-bytesToWrite);
      }
    }

    @Override
    public final synchronized void flush() throws IOException {
      if (streamClosed) {
        throw new IOException(fsPath + " stream object is closed.");
      }

      if (offset > 0) {
        try {
          commit(data, 0, offset, eof);
        } catch (IOException e) {
          hadError = true;
          throw e;
        }
      }

      offset = 0;
    }

    @Override
    public final synchronized void close() throws IOException {
      // Stream is closed earlier, return quietly.
      if(streamClosed) {
        return;
      }

      if (featureRedirectOff) {
        eof = true;
      }

      boolean flushedSomething = false;
      if (hadError) {
        // No point proceeding further since the error has occurred and
        // stream would be required to upload again.
        streamClosed = true;
        return;
      } else {
        flushedSomething = offset > 0;
        try {
          flush();
        } finally {
          streamClosed = true;
        }
      }

      if (featureRedirectOff) {
        // If we didn't flush anything from our internal buffer, we have to
        // call the service again
        // with an empty payload and flush=true in the url
        if (!flushedSomething) {
          try {
            commit(null, 0, ADLConfKeys.KB, true);
          } finally {
            streamClosed = true;
          }
        }
      }
    }

    private void commit(byte[] buffer, int off, int len, boolean endOfFile)
        throws IOException {
      OutputStream out = null;
      try {
        if (featureRedirectOff) {
          AppendADLNoRedirectParam skipRedirect = new AppendADLNoRedirectParam(
              true);
          Param<?, ?>[] tmpParam = featureFlushWhenEOF ?
              new Param<?, ?>[parameters.length + 3] :
              new Param<?, ?>[parameters.length + 1];
          System.arraycopy(parameters, 0, tmpParam, 0, parameters.length);
          tmpParam[parameters.length] = skipRedirect;
          if (featureFlushWhenEOF) {
            tmpParam[parameters.length + 1] = new ADLFlush(endOfFile);
            tmpParam[parameters.length + 2] = new OffsetParam(length);
          }

          out = new FsPathOutputStreamRunner(ADLPostOpParam.Op.APPEND, fsPath,
              len, tmpParam).run();
        } else {
          out = new FsPathOutputStreamRunner(ADLPostOpParam.Op.APPEND, fsPath,
              len, parameters).run();
        }

        if (buffer != null) {
          out.write(buffer, off, len);
          length += len;
        }
      } finally {
        if (out != null) {
          out.close();
        }
      }
    }

    private byte[] getBuffer() {
      // Switch between the first and second buffer
      dataBuffers = new byte[bufSize];
      return dataBuffers;
    }
  }

  /**
   * Read data from backend in chunks instead of persistent connection. This
   * is to avoid slow reader causing socket
   * timeout.
   */
  protected class BatchByteArrayInputStream extends FSInputStream {

    private static final int SIZE4MB = 4 * 1024 * 1024;
    private final URL runner;
    private byte[] data = null;
    private long validDataHoldingSize = 0;
    private int bufferOffset = 0;
    private long currentFileOffset = 0;
    private long nextFileOffset = 0;
    private long fileSize = 0;
    private StreamState state = StreamState.Initial;
    private int maxBufferSize;
    private int maxConcurrentConnection;
    private Path fsPath;
    private boolean streamIsClosed;
    private Future[] subtasks = null;

    BatchByteArrayInputStream(URL url, Path p, int bufferSize,
        int concurrentConnection) throws IOException {
      this.runner = url;
      fsPath = p;
      FileStatus fStatus = getFileStatus(fsPath);
      if (!fStatus.isFile()) {
        throw new IOException("Cannot open the directory " + p + " for " +
            "reading");
      }
      fileSize = fStatus.getLen();
      this.maxBufferSize = bufferSize;
      this.maxConcurrentConnection = concurrentConnection;
      this.streamIsClosed = false;
    }

    @Override
    public synchronized final int read(long position, byte[] buffer, int offset,
        int length) throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      long oldPos = this.getPos();

      int nread1;
      try {
        this.seek(position);
        nread1 = this.read(buffer, offset, length);
      } finally {
        this.seek(oldPos);
      }

      return nread1;
    }

    @Override
    public synchronized final int read() throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      int status = doBufferAvailabilityCheck();
      if (status == -1) {
        return status;
      }
      int ch = data[bufferOffset++] & (0xff);
      if (statistics != null) {
        statistics.incrementBytesRead(1);
      }
      return ch;
    }

    @Override
    public synchronized final void readFully(long position, byte[] buffer,
        int offset, int length) throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }

      super.readFully(position, buffer, offset, length);
      if (statistics != null) {
        statistics.incrementBytesRead(length);
      }
    }

    @Override
    public synchronized final int read(byte[] b, int off, int len)
        throws IOException {
      if (b == null) {
        throw new IllegalArgumentException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }

      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      int status = doBufferAvailabilityCheck();
      if (status == -1) {
        return status;
      }

      int byteRead = 0;
      long availableBytes = validDataHoldingSize - off;
      long requestedBytes = bufferOffset + len - off;
      if (requestedBytes <= availableBytes) {
        System.arraycopy(data, bufferOffset, b, off, len);
        bufferOffset += len;
        byteRead = len;
      } else {
        byteRead = super.read(b, off, len);
      }

      if (statistics != null) {
        statistics.incrementBytesRead(byteRead);
      }

      return byteRead;
    }

    private int doBufferAvailabilityCheck() throws IOException {
      if (state == StreamState.Initial) {
        validDataHoldingSize = fill(nextFileOffset);
      }

      long dataReloadSize = 0;
      switch ((int) validDataHoldingSize) {
      case -1:
        state = StreamState.StreamEnd;
        return -1;
      case 0:
        dataReloadSize = fill(nextFileOffset);
        if (dataReloadSize <= 0) {
          state = StreamState.StreamEnd;
          return (int) dataReloadSize;
        } else {
          validDataHoldingSize = dataReloadSize;
        }
        break;
      default:
        break;
      }

      if (bufferOffset >= validDataHoldingSize) {
        dataReloadSize = fill(nextFileOffset);
      }

      if (bufferOffset >= ((dataReloadSize == 0) ?
          validDataHoldingSize :
          dataReloadSize)) {
        state = StreamState.StreamEnd;
        return -1;
      }

      validDataHoldingSize = ((dataReloadSize == 0) ?
          validDataHoldingSize :
          dataReloadSize);
      state = StreamState.DataCachedInLocalBuffer;
      return 0;
    }

    private long fill(final long off) throws IOException {
      if (state == StreamState.StreamEnd) {
        return -1;
      }

      if (fileSize <= off) {
        state = StreamState.StreamEnd;
        return -1;
      }
      int len = maxBufferSize;
      long fileOffset = 0;
      boolean isEntireFileCached = true;
      if ((fileSize <= maxBufferSize)) {
        len = (int) fileSize;
        currentFileOffset = 0;
        nextFileOffset = 0;
      } else {
        if (len > (fileSize - off)) {
          len = (int) (fileSize - off);
        }

        synchronized (BufferManager.getLock()) {
          if (BufferManager.getInstance()
              .hasValidDataForOffset(fsPath.toString(), off)) {
            len = (int) (
                BufferManager.getInstance().getBufferOffset() + BufferManager
                    .getInstance().getBufferSize() - (int) off);
          }
        }

        if (len <= 0) {
          len = maxBufferSize;
        }
        fileOffset = off;
        isEntireFileCached = false;
      }

      data = null;
      BufferManager bm = BufferManager.getInstance();
      data = bm.getEmpty(len);
      boolean fetchDataOverNetwork = false;
      synchronized (BufferManager.getLock()) {
        if (bm.hasData(fsPath.toString(), fileOffset, len)) {
          try {
            bm.get(data, fileOffset);
            validDataHoldingSize = data.length;
            currentFileOffset = fileOffset;
          } catch (ArrayIndexOutOfBoundsException e) {
            fetchDataOverNetwork = true;
          }
        } else {
          fetchDataOverNetwork = true;
        }
      }

      if (fetchDataOverNetwork) {
        int splitSize = getSplitSize(len);
        try {
          validDataHoldingSize = fillDataConcurrently(data, len, fileOffset,
              splitSize);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted filling buffer", e);
        }

        synchronized (BufferManager.getLock()) {
          bm.add(data, fsPath.toString(), fileOffset);
        }
        currentFileOffset = nextFileOffset;
      }

      nextFileOffset += validDataHoldingSize;
      state = StreamState.DataCachedInLocalBuffer;
      bufferOffset = isEntireFileCached ? (int) off : 0;
      return validDataHoldingSize;
    }

    int getSplitSize(int size) {
      if (size <= SIZE4MB) {
        return 1;
      }

      // Not practical
      if (size > maxBufferSize) {
        size = maxBufferSize;
      }

      int equalBufferSplit = Math.max(Math.round(size / SIZE4MB), 1);
      int splitSize = Math.min(equalBufferSplit, maxConcurrentConnection);
      return splitSize;
    }

    @Override
    public synchronized final void seek(long pos) throws IOException {
      if (pos == -1) {
        throw new IOException("Bad offset, cannot seek to " + pos);
      }

      BufferManager bm = BufferManager.getInstance();
      synchronized (BufferManager.getLock()) {
        if (bm.hasValidDataForOffset(fsPath.toString(), pos)) {
          state = StreamState.DataCachedInLocalBuffer;
        } else if (pos >= 0) {
          state = StreamState.Initial;
        }
      }

      long availableBytes = (currentFileOffset + validDataHoldingSize);

      // Check if this position falls under buffered data
      if (pos < currentFileOffset || availableBytes <= 0) {
        validDataHoldingSize = 0;
        currentFileOffset = pos;
        nextFileOffset = pos;
        bufferOffset = 0;
        return;
      }

      if (pos < availableBytes && pos >= currentFileOffset) {
        state = StreamState.DataCachedInLocalBuffer;
        bufferOffset = (int) (pos - currentFileOffset);
      } else {
        validDataHoldingSize = 0;
        currentFileOffset = pos;
        nextFileOffset = pos;
        bufferOffset = 0;
      }
    }

    @Override
    public synchronized final long getPos() throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      return bufferOffset + currentFileOffset;
    }

    @Override
    public synchronized final int available() throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      return Integer.MAX_VALUE;
    }

    @Override
    public final boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @SuppressWarnings("unchecked")
    private int fillDataConcurrently(byte[] byteArray, int length,
        long globalOffset, int splitSize)
        throws IOException, InterruptedException {
      ExecutorService executor = Executors.newFixedThreadPool(splitSize);
      subtasks = new Future[splitSize];
      for (int i = 0; i < splitSize; i++) {
        int offset = i * (length / splitSize);
        int splitLength = (splitSize == (i + 1)) ?
            (length / splitSize) + (length % splitSize) :
            (length / splitSize);
        subtasks[i] = executor.submit(
            new BackgroundReadThread(byteArray, offset, splitLength,
                globalOffset + offset));
      }

      executor.shutdown();
      // wait until all tasks are finished
      executor.awaitTermination(ADLConfKeys.DEFAULT_TIMEOUT_IN_SECONDS,
          TimeUnit.SECONDS);

      int totalBytePainted = 0;
      for (int i = 0; i < splitSize; ++i) {
        try {
          totalBytePainted += (Integer) subtasks[i].get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e.getCause());
        } catch (ExecutionException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e.getCause());
        }
      }

      if (totalBytePainted != length) {
        throw new IOException("Expected " + length + " bytes, Got " +
            totalBytePainted + " bytes");
      }

      return totalBytePainted;
    }

    @Override
    public synchronized final void close() throws IOException {
      synchronized (BufferManager.getLock()) {
        BufferManager.getInstance().clear();
      }
      //need to cleanup the above code the stream and connection close doesn't
      // happen here
      //flag set to mark close happened, cannot use the stream once closed
      streamIsClosed = true;
    }

    /**
     * Reads data from the ADL backend from the specified global offset and
     * given
     * length. Read data from ADL backend is copied to buffer array from the
     * offset value specified.
     *
     * @param buffer       Store read data from ADL backend in the buffer.
     * @param offset       Store read data from ADL backend in the buffer
     *                     from the
     *                     offset.
     * @param length       Size of the data read from the ADL backend.
     * @param globalOffset Read data from file offset.
     * @return Number of bytes read from the ADL backend
     * @throws IOException For any intermittent server issues or internal
     *                     failures.
     */
    private int fillUpData(byte[] buffer, int offset, int length,
        long globalOffset) throws IOException {
      int totalBytesRead = 0;
      final URL offsetUrl = new URL(
          runner + "&" + new OffsetParam(String.valueOf(globalOffset)) + "&"
              + new LengthParam(String.valueOf(length)));
      HttpURLConnection conn = new URLRunner(GetOpParam.Op.OPEN, offsetUrl,
          true).run();
      InputStream in = conn.getInputStream();
      try {
        int bytesRead = 0;
        while ((bytesRead = in.read(buffer, (int) offset + totalBytesRead,
            (int) (length - totalBytesRead))) > 0) {
          totalBytesRead += bytesRead;
        }

        // InputStream must be fully consumed to enable http keep-alive
        if (bytesRead == 0) {
          // Looking for EOF marker byte needs to be read.
          if (in.read() != -1) {
            throw new SocketException(
                "Server returned more than requested data.");
          }
        }
      } finally {
        in.close();
        conn.disconnect();
      }

      return totalBytesRead;
    }

    private class BackgroundReadThread implements Callable {

      private final byte[] data;
      private int offset;
      private int length;
      private long globalOffset;

      BackgroundReadThread(byte[] buffer, int off, int size, long position) {
        this.data = buffer;
        this.offset = off;
        this.length = size;
        this.globalOffset = position;
      }

      public Object call() throws IOException {
        return fillUpData(data, offset, length, globalOffset);
      }
    }
  }
}
