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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * An implementation of the abstract class {@link EditLogOutputStream}, which
 * stores edits in a local file.
 */
@InterfaceAudience.Private
public class EditLogFileOutputStream extends EditLogOutputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(EditLogFileOutputStream.class);
  public static final int MIN_PREALLOCATION_LENGTH = 1024 * 1024;

  private File file;
  private FileOutputStream fp; // file stream for storing edit logs
  private FileChannel fc; // channel of the file stream for sync
  private EditsDoubleBuffer doubleBuf;
  static final ByteBuffer fill = ByteBuffer.allocateDirect(MIN_PREALLOCATION_LENGTH);
  private boolean shouldSyncWritesAndSkipFsync = false;

  private static boolean shouldSkipFsyncForTests = false;

  static {
    fill.position(0);
    for (int i = 0; i < fill.capacity(); i++) {
      fill.put(FSEditLogOpCodes.OP_INVALID.getOpCode());
    }
  }

  /**
   * Creates output buffers and file object.
   * 
   * @param conf
   *          Configuration object
   * @param name
   *          File name to store edit log
   * @param size
   *          Size of flush buffer
   * @throws IOException
   */
  public EditLogFileOutputStream(Configuration conf, File name, int size)
      throws IOException {
    super();
    shouldSyncWritesAndSkipFsync = conf.getBoolean(
            DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH,
            DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH_DEFAULT);

    file = name;
    doubleBuf = new EditsDoubleBuffer(size);
    RandomAccessFile rp;
    if (shouldSyncWritesAndSkipFsync) {
      rp = new RandomAccessFile(name, "rwd");
    } else {
      rp = new RandomAccessFile(name, "rw");
    }
    try {
      fp = new FileOutputStream(rp.getFD()); // open for append
    } catch (IOException e) {
      IOUtils.closeStream(rp);
      throw e;
    }
    fc = rp.getChannel();
    fc.position(fc.size());
  }

  @Override
  public void write(FSEditLogOp op) throws IOException {
    doubleBuf.writeOp(op, getCurrentLogVersion());
  }

  /**
   * Write a transaction to the stream. The serialization format is:
   * <ul>
   *   <li>the opcode (byte)</li>
   *   <li>the transaction id (long)</li>
   *   <li>the actual Writables for the transaction</li>
   * </ul>
   * */
  @Override
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    doubleBuf.writeRaw(bytes, offset, length);
  }

  /**
   * Create empty edits logs file.
   */
  @Override
  public void create(int layoutVersion) throws IOException {
    fc.truncate(0);
    fc.position(0);
    writeHeader(layoutVersion, doubleBuf.getCurrentBuf());
    setReadyToFlush();
    flush();
    setCurrentLogVersion(layoutVersion);
  }

  /**
   * Write header information for this EditLogFileOutputStream to the provided
   * DataOutputSream.
   * 
   * @param layoutVersion the LayoutVersion of the EditLog
   * @param out the output stream to write the header to.
   * @throws IOException in the event of error writing to the stream.
   */
  @VisibleForTesting
  public static void writeHeader(int layoutVersion, DataOutputStream out)
      throws IOException {
    out.writeInt(layoutVersion);
    LayoutFlags.write(out);
  }

  @Override
  public void close() throws IOException {
    if (fp == null) {
      throw new IOException("Trying to use aborted output stream");
    }

    try {
      // close should have been called after all pending transactions
      // have been flushed & synced.
      // if already closed, just skip
      if (doubleBuf != null) {
        doubleBuf.close();
        doubleBuf = null;
      }
      
      // remove any preallocated padding bytes from the transaction log.
      if (fc != null && fc.isOpen()) {
        fc.truncate(fc.position());
        fc.close();
        fc = null;
      }
      fp.close();
      fp = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, fc, fp);
      doubleBuf = null;
      fc = null;
      fp = null;
    }
    fp = null;
  }
  
  @Override
  public void abort() throws IOException {
    if (fp == null) {
      return;
    }
    IOUtils.cleanupWithLogger(LOG, fp);
    fp = null;
  }

  /**
   * All data that has been written to the stream so far will be flushed. New
   * data can be still written to the stream while flushing is performed.
   */
  @Override
  public void setReadyToFlush() throws IOException {
    doubleBuf.setReadyToFlush();
  }

  /**
   * Flush ready buffer to persistent store. currentBuffer is not flushed as it
   * accumulates new log records while readyBuffer will be flushed and synced.
   */
  @Override
  public void flushAndSync(boolean durable) throws IOException {
    if (fp == null) {
      throw new IOException("Trying to use aborted output stream");
    }
    if (doubleBuf.isFlushed()) {
      LOG.info("Nothing to flush");
      return;
    }
    preallocate(); // preallocate file if necessary
    doubleBuf.flushTo(fp);
    if (durable && !shouldSkipFsyncForTests && !shouldSyncWritesAndSkipFsync) {
      fc.force(false); // metadata updates not needed
    }
  }

  /**
   * @return true if the number of buffered data exceeds the intial buffer size
   */
  @Override
  public boolean shouldForceSync() {
    return doubleBuf.shouldForceSync();
  }

  private void preallocate() throws IOException {
    long position = fc.position();
    long size = fc.size();
    int bufSize = doubleBuf.getReadyBuf().getLength();
    long need = bufSize - (size - position);
    if (need <= 0) {
      return;
    }
    long oldSize = size;
    long total = 0;
    long fillCapacity = fill.capacity();
    while (need > 0) {
      fill.position(0);
      IOUtils.writeFully(fc, fill, size);
      need -= fillCapacity;
      size += fillCapacity;
      total += fillCapacity;
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("Preallocated " + total + " bytes at the end of " +
      		"the edit log (offset " + oldSize + ")");
    }
  }

  /**
   * Returns the file associated with this stream.
   */
  File getFile() {
    return file;
  }
  
  @Override
  public String toString() {
    return "EditLogFileOutputStream(" + file + ")";
  }

  /**
   * @return true if this stream is currently open.
   */
  public boolean isOpen() {
    return fp != null;
  }
  
  @VisibleForTesting
  public void setFileChannelForTesting(FileChannel fc) {
    this.fc = fc;
  }
  
  @VisibleForTesting
  public FileChannel getFileChannelForTesting() {
    return fc;
  }
  
  /**
   * For the purposes of unit tests, we don't need to actually
   * write durably to disk. So, we can skip the fsync() calls
   * for a speed improvement.
   * @param skip true if fsync should <em>not</em> be called
   */
  @VisibleForTesting
  public static void setShouldSkipFsyncForTesting(boolean skip) {
    shouldSkipFsyncForTests = skip;
  }
}
