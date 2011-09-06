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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.DataInputStream;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import com.google.common.annotations.VisibleForTesting;

/**
 * An implementation of the abstract class {@link EditLogInputStream}, which
 * reads edits from a local file.
 */
class EditLogFileInputStream extends EditLogInputStream {
  private final File file;
  private final FileInputStream fStream;
  final private long firstTxId;
  final private long lastTxId;
  private final int logVersion;
  private final FSEditLogOp.Reader reader;
  private final FSEditLogLoader.PositionTrackingInputStream tracker;
  
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
    this(name, HdfsConstants.INVALID_TXID, HdfsConstants.INVALID_TXID);
  }

  /**
   * Open an EditLogInputStream for the given file.
   * @param name filename to open
   * @param firstTxId first transaction found in file
   * @param lastTxId last transaction id found in file
   * @throws LogHeaderCorruptException if the header is either missing or
   *         appears to be corrupt/truncated
   * @throws IOException if an actual IO error occurs while reading the
   *         header
   */
  EditLogFileInputStream(File name, long firstTxId, long lastTxId)
      throws LogHeaderCorruptException, IOException {
    file = name;
    fStream = new FileInputStream(name);

    BufferedInputStream bin = new BufferedInputStream(fStream);
    tracker = new FSEditLogLoader.PositionTrackingInputStream(bin);
    DataInputStream in = new DataInputStream(tracker);

    try {
      logVersion = readLogVersion(in);
    } catch (EOFException eofe) {
      throw new LogHeaderCorruptException("No header found in log");
    }

    reader = new FSEditLogOp.Reader(in, logVersion);
    this.firstTxId = firstTxId;
    this.lastTxId = lastTxId;
  }

  @Override
  public long getFirstTxId() throws IOException {
    return firstTxId;
  }
  
  @Override
  public long getLastTxId() throws IOException {
    return lastTxId;
  }

  @Override // JournalStream
  public String getName() {
    return file.getPath();
  }

  @Override // JournalStream
  public JournalType getType() {
    return JournalType.FILE;
  }

  @Override
  public FSEditLogOp readOp() throws IOException {
    return reader.readOp();
  }

  @Override
  public int getVersion() throws IOException {
    return logVersion;
  }

  @Override
  public long getPosition() {
    return tracker.getPos();
  }

  @Override
  public void close() throws IOException {
    fStream.close();
  }

  @Override
  long length() throws IOException {
    // file size + size of both buffers
    return file.length();
  }
  
  @Override
  public String toString() {
    return getName();
  }

  static FSEditLogLoader.EditLogValidation validateEditLog(File file) throws IOException {
    EditLogFileInputStream in;
    try {
      in = new EditLogFileInputStream(file);
    } catch (LogHeaderCorruptException corrupt) {
      // If it's missing its header, this is equivalent to no transactions
      FSImage.LOG.warn("Log at " + file + " has no valid header",
          corrupt);
      return new FSEditLogLoader.EditLogValidation(0, HdfsConstants.INVALID_TXID, 
                                                   HdfsConstants.INVALID_TXID);
    }
    
    try {
      return FSEditLogLoader.validateEditLog(in);
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
  static int readLogVersion(DataInputStream in)
      throws IOException, LogHeaderCorruptException {
    int logVersion;
    try {
      logVersion = in.readInt();
    } catch (EOFException eofe) {
      throw new LogHeaderCorruptException(
          "Reached EOF when reading log header");
    }
    if (logVersion < HdfsConstants.LAYOUT_VERSION) { // future version
      throw new LogHeaderCorruptException(
          "Unexpected version of the file system log file: "
          + logVersion + ". Current version = "
          + HdfsConstants.LAYOUT_VERSION + ".");
    }
    assert logVersion <= Storage.LAST_UPGRADABLE_LAYOUT_VERSION :
      "Unsupported version " + logVersion;
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
}
