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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HttpGetFailedException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * An implementation of the abstract class {@link EditLogInputStream}, which
 * reads edits from a file. That file may be either on the local disk or
 * accessible via a URL.
 */
@InterfaceAudience.Private
public class EditLogFileInputStream extends EditLogInputStream {
  private final LogSource log;
  private final long firstTxId;
  private final long lastTxId;
  private final boolean isInProgress;
  private int maxOpSize;
  static private enum State {
    UNINIT,
    OPEN,
    CLOSED
  }
  private State state = State.UNINIT;
  private InputStream fStream = null;
  private int logVersion = 0;
  private FSEditLogOp.Reader reader = null;
  private FSEditLogLoader.PositionTrackingInputStream tracker = null;
  private DataInputStream dataIn = null;
  static final Logger LOG = LoggerFactory.getLogger(EditLogInputStream.class);
  
  /**
   * Open an EditLogInputStream for the given file.
   * The file is pretransactional, so has no txids
   * @param name filename to open
   * @throws LogHeaderCorruptException if the header is either missing or
   *         appears to be corrupt/truncated
   * @throws IOException if an actual IO error occurs while reading the
   *         header
   */
  EditLogFileInputStream(File name)
      throws LogHeaderCorruptException, IOException {
    this(name, HdfsServerConstants.INVALID_TXID, HdfsServerConstants.INVALID_TXID, false);
  }

  /**
   * Open an EditLogInputStream for the given file.
   * @param name filename to open
   * @param firstTxId first transaction found in file
   * @param lastTxId last transaction id found in file
   */
  public EditLogFileInputStream(File name, long firstTxId, long lastTxId,
      boolean isInProgress) {
    this(new FileLog(name), firstTxId, lastTxId, isInProgress);
  }
  
  /**
   * Open an EditLogInputStream for the given URL.
   *
   * @param connectionFactory
   *          the URLConnectionFactory used to create the connection.
   * @param url
   *          the url hosting the log
   * @param startTxId
   *          the expected starting txid
   * @param endTxId
   *          the expected ending txid
   * @param inProgress
   *          whether the log is in-progress
   * @return a stream from which edits may be read
   */
  public static EditLogInputStream fromUrl(
      URLConnectionFactory connectionFactory, URL url, long startTxId,
      long endTxId, boolean inProgress) {
    return new EditLogFileInputStream(new URLLog(connectionFactory, url),
        startTxId, endTxId, inProgress);
  }
  
  private EditLogFileInputStream(LogSource log,
      long firstTxId, long lastTxId,
      boolean isInProgress) {
      
    this.log = log;
    this.firstTxId = firstTxId;
    this.lastTxId = lastTxId;
    this.isInProgress = isInProgress;
    this.maxOpSize = DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_DEFAULT;
  }

  private void init(boolean verifyLayoutVersion)
      throws LogHeaderCorruptException, IOException {
    Preconditions.checkState(state == State.UNINIT);
    BufferedInputStream bin = null;
    try {
      fStream = log.getInputStream();
      bin = new BufferedInputStream(fStream);
      tracker = new FSEditLogLoader.PositionTrackingInputStream(bin);
      dataIn = new DataInputStream(tracker);
      try {
        logVersion = readLogVersion(dataIn, verifyLayoutVersion);
      } catch (EOFException eofe) {
        throw new LogHeaderCorruptException("No header found in log");
      }
      // We assume future layout will also support ADD_LAYOUT_FLAGS
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.ADD_LAYOUT_FLAGS, logVersion) ||
          logVersion < NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION) {
        try {
          LayoutFlags.read(dataIn);
        } catch (EOFException eofe) {
          throw new LogHeaderCorruptException("EOF while reading layout " +
              "flags from log");
        }
      }
      reader = FSEditLogOp.Reader.create(dataIn, tracker, logVersion);
      reader.setMaxOpSize(maxOpSize);
      state = State.OPEN;
    } finally {
      if (reader == null) {
        IOUtils.cleanupWithLogger(LOG, dataIn, tracker, bin, fStream);
        state = State.CLOSED;
      }
    }
  }

  @Override
  public long getFirstTxId() {
    return firstTxId;
  }
  
  @Override
  public long getLastTxId() {
    return lastTxId;
  }

  @Override
  public String getName() {
    return log.getName();
  }

  private FSEditLogOp nextOpImpl(boolean skipBrokenEdits) throws IOException {
    FSEditLogOp op = null;
    switch (state) {
    case UNINIT:
      try {
        init(true);
      } catch (Throwable e) {
        LOG.error("caught exception initializing " + this, e);
        if (skipBrokenEdits) {
          return null;
        }
        Throwables.propagateIfPossible(e, IOException.class);
      }
      Preconditions.checkState(state != State.UNINIT);
      return nextOpImpl(skipBrokenEdits);
    case OPEN:
      op = reader.readOp(skipBrokenEdits);
      if ((op != null) && (op.hasTransactionId())) {
        long txId = op.getTransactionId();
        if ((txId >= lastTxId) &&
            (lastTxId != HdfsServerConstants.INVALID_TXID)) {
          //
          // Sometimes, the NameNode crashes while it's writing to the
          // edit log.  In that case, you can end up with an unfinalized edit log
          // which has some garbage at the end.
          // JournalManager#recoverUnfinalizedSegments will finalize these
          // unfinished edit logs, giving them a defined final transaction 
          // ID.  Then they will be renamed, so that any subsequent
          // readers will have this information.
          //
          // Since there may be garbage at the end of these "cleaned up"
          // logs, we want to be sure to skip it here if we've read everything
          // we were supposed to read out of the stream.
          // So we force an EOF on all subsequent reads.
          //
          long skipAmt = log.length() - tracker.getPos();
          if (skipAmt > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skipping " + skipAmt + " bytes at the end " +
                  "of edit log  '" + getName() + "': reached txid " + txId +
                  " out of " + lastTxId);
            }
            tracker.clearLimit();
            IOUtils.skipFully(tracker, skipAmt);
          }
        }
      }
      break;
      case CLOSED:
        break; // return null
    }
    return op;
  }

  @Override
  protected long scanNextOp() throws IOException {
    Preconditions.checkState(state == State.OPEN);
    FSEditLogOp cachedNext = getCachedOp();
    return cachedNext == null ? reader.scanOp() : cachedNext.txid;
  }

  @Override
  protected FSEditLogOp nextOp() throws IOException {
    return nextOpImpl(false);
  }

  @Override
  protected FSEditLogOp nextValidOp() {
    try {
      return nextOpImpl(true);
    } catch (Throwable e) {
      LOG.error("nextValidOp: got exception while reading " + this, e);
      return null;
    }
  }

  @Override
  public int getVersion(boolean verifyVersion) throws IOException {
    if (state == State.UNINIT) {
      init(verifyVersion);
    }
    return logVersion;
  }

  @Override
  public long getPosition() {
    if (state == State.OPEN) {
      return tracker.getPos();
    } else {
      return 0;
    }
  }

  @Override
  public void close() throws IOException {
    if (state == State.OPEN) {
      dataIn.close();
    }
    state = State.CLOSED;
  }

  @Override
  public long length() throws IOException {
    // file size + size of both buffers
    return log.length();
  }
  
  @Override
  public boolean isInProgress() {
    return isInProgress;
  }
  
  @Override
  public String toString() {
    return getName();
  }

  /**
   * @param file          File being scanned and validated.
   * @param maxTxIdToScan Maximum Tx ID to try to scan.
   *                      The scan returns after reading this or a higher
   *                      ID. The file portion beyond this ID is
   *                      potentially being updated.
   * @return Result of the validation
   * @throws IOException
   */
  static FSEditLogLoader.EditLogValidation scanEditLog(File file,
      long maxTxIdToScan, boolean verifyVersion)
      throws IOException {
    EditLogFileInputStream in;
    try {
      in = new EditLogFileInputStream(file);
      // read the header, initialize the inputstream, but do not check the
      // layoutversion
      in.getVersion(verifyVersion);
    } catch (LogHeaderCorruptException e) {
      LOG.warn("Log file " + file + " has no valid header", e);
      return new FSEditLogLoader.EditLogValidation(0,
          HdfsServerConstants.INVALID_TXID, true);
    }

    try {
      return FSEditLogLoader.scanEditLog(in, maxTxIdToScan);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  /**
   * Read the header of fsedit log
   * @param in fsedit stream
   * @return the edit log version number
   * @throws IOException if error occurs
   */
  @VisibleForTesting
  static int readLogVersion(DataInputStream in, boolean verifyLayoutVersion)
      throws IOException, LogHeaderCorruptException {
    int logVersion;
    try {
      logVersion = in.readInt();
    } catch (EOFException eofe) {
      throw new LogHeaderCorruptException(
          "Reached EOF when reading log header");
    }
    if (verifyLayoutVersion &&
        (logVersion < HdfsServerConstants.NAMENODE_LAYOUT_VERSION || // future version
         logVersion > Storage.LAST_UPGRADABLE_LAYOUT_VERSION)) { // unsupported
      throw new LogHeaderCorruptException(
          "Unexpected version of the file system log file: "
          + logVersion + ". Current version = "
          + HdfsServerConstants.NAMENODE_LAYOUT_VERSION + ".");
    }
    return logVersion;
  }
  
  /**
   * Exception indicating that the header of an edits log file is
   * corrupted. This can be because the header is not present,
   * or because the header data is invalid (eg claims to be
   * over a newer version than the running NameNode)
   */
  static class LogHeaderCorruptException extends IOException {
    private static final long serialVersionUID = 1L;

    private LogHeaderCorruptException(String msg) {
      super(msg);
    }
  }
  
  private interface LogSource {
    public InputStream getInputStream() throws IOException;
    public long length();
    public String getName();
  }
  
  private static class FileLog implements LogSource {
    private final File file;
    
    public FileLog(File file) {
      this.file = file;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return new FileInputStream(file);
    }

    @Override
    public long length() {
      return file.length();
    }

    @Override
    public String getName() {
      return file.getPath();
    }
  }

  private static class URLLog implements LogSource {
    private final URL url;
    private long advertisedSize = -1;

    private final static String CONTENT_LENGTH = "Content-Length";
    private final URLConnectionFactory connectionFactory;
    private final boolean isSpnegoEnabled;

    public URLLog(URLConnectionFactory connectionFactory, URL url) {
      this.connectionFactory = connectionFactory;
      this.isSpnegoEnabled = UserGroupInformation.isSecurityEnabled();
      this.url = url;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return SecurityUtil.doAsCurrentUser(
          new PrivilegedExceptionAction<InputStream>() {
            @Override
            public InputStream run() throws IOException {
              HttpURLConnection connection;
              try {
                connection = (HttpURLConnection)
                    connectionFactory.openConnection(url, isSpnegoEnabled);
              } catch (AuthenticationException e) {
                throw new IOException(e);
              }
              
              if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new HttpGetFailedException(
                    "Fetch of " + url +
                    " failed with status code " + connection.getResponseCode() +
                    "\nResponse message:\n" + connection.getResponseMessage(),
                    connection);
              }
        
              String contentLength = connection.getHeaderField(CONTENT_LENGTH);
              if (contentLength != null) {
                advertisedSize = Long.parseLong(contentLength);
                if (advertisedSize <= 0) {
                  throw new IOException("Invalid " + CONTENT_LENGTH + " header: " +
                      contentLength);
                }
              } else {
                throw new IOException(CONTENT_LENGTH + " header is not provided " +
                                      "by the server when trying to fetch " + url);
              }
        
              return connection.getInputStream();
            }
          });
    }

    @Override
    public long length() {
      return advertisedSize;
    }

    @Override
    public String getName() {
      return url.toString();
    }
  }

  @Override
  public void setMaxOpSize(int maxOpSize) {
    this.maxOpSize = maxOpSize;
    if (reader != null) {
      reader.setMaxOpSize(maxOpSize);
    }
  }

  @Override
  public boolean isLocalLog() {
    return log instanceof FileLog;
  }
}
