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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.IOUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * An implementation of the abstract class {@link EditLogOutputStream}, which
 * stores edits in a local file.
 */
class EditLogFileOutputStream extends EditLogOutputStream {
  private static Log LOG = LogFactory.getLog(EditLogFileOutputStream.class);;

  private static int EDITS_FILE_HEADER_SIZE_BYTES = Integer.SIZE / Byte.SIZE;

  private File file;
  private FileOutputStream fp; // file stream for storing edit logs
  private FileChannel fc; // channel of the file stream for sync
  private EditsDoubleBuffer doubleBuf;
  static ByteBuffer fill = ByteBuffer.allocateDirect(1024 * 1024); // preallocation, 1MB

  static {
    fill.position(0);
    for (int i = 0; i < fill.capacity(); i++) {
      fill.put(FSEditLogOpCodes.OP_INVALID.getOpCode());
    }
  }

  /**
   * Creates output buffers and file object.
   * 
   * @param name
   *          File name to store edit log
   * @param size
   *          Size of flush buffer
   * @throws IOException
   */
  EditLogFileOutputStream(File name, int size) throws IOException {
    super();
    file = name;
    doubleBuf = new EditsDoubleBuffer(size);
    RandomAccessFile rp = new RandomAccessFile(name, "rw");
    fp = new FileOutputStream(rp.getFD()); // open for append
    fc = rp.getChannel();
    fc.position(fc.size());
  }

  @Override // JournalStream
  public String getName() {
    return file.getPath();
  }

  @Override // JournalStream
  public JournalType getType() {
    return JournalType.FILE;
  }

  /** {@inheritDoc} */
  @Override
  void write(FSEditLogOp op) throws IOException {
    doubleBuf.writeOp(op);
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
  void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    doubleBuf.writeRaw(bytes, offset, length);
  }

  /**
   * Create empty edits logs file.
   */
  @Override
  void create() throws IOException {
    fc.truncate(0);
    fc.position(0);
    doubleBuf.getCurrentBuf().writeInt(HdfsConstants.LAYOUT_VERSION);
    setReadyToFlush();
    flush();
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
      
      // remove the last INVALID marker from transaction log.
      if (fc != null && fc.isOpen()) {
        fc.truncate(fc.position());
        fc.close();
        fc = null;
      }
      if (fp != null) {
        fp.close();
        fp = null;
      }
    } finally {
      IOUtils.cleanup(FSNamesystem.LOG, fc, fp);
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
    IOUtils.cleanup(LOG, fp);
    fp = null;
  }

  /**
   * All data that has been written to the stream so far will be flushed. New
   * data can be still written to the stream while flushing is performed.
   */
  @Override
  void setReadyToFlush() throws IOException {
    doubleBuf.getCurrentBuf().write(FSEditLogOpCodes.OP_INVALID.getOpCode()); // insert eof marker
    doubleBuf.setReadyToFlush();
  }

  /**
   * Flush ready buffer to persistent store. currentBuffer is not flushed as it
   * accumulates new log records while readyBuffer will be flushed and synced.
   */
  @Override
  protected void flushAndSync() throws IOException {
    if (fp == null) {
      throw new IOException("Trying to use aborted output stream");
    }
    
    preallocate(); // preallocate file if necessary
    doubleBuf.flushTo(fp);
    fc.force(false); // metadata updates not needed because of preallocation
    fc.position(fc.position() - 1); // skip back the end-of-file marker
  }

  /**
   * @return true if the number of buffered data exceeds the intial buffer size
   */
  @Override
  public boolean shouldForceSync() {
    return doubleBuf.shouldForceSync();
  }
  
  /**
   * Return the size of the current edit log including buffered data.
   */
  @Override
  long length() throws IOException {
    // file size - header size + size of both buffers
    return fc.size() - EDITS_FILE_HEADER_SIZE_BYTES + 
      doubleBuf.countBufferedBytes();
  }

  // allocate a big chunk of data
  private void preallocate() throws IOException {
    long position = fc.position();
    if (position + 4096 >= fc.size()) {
      if(FSNamesystem.LOG.isDebugEnabled()) {
        FSNamesystem.LOG.debug("Preallocating Edit log, current size "
            + fc.size());
      }
      fill.position(0);
      int written = fc.write(fill, position);
      if(FSNamesystem.LOG.isDebugEnabled()) {
        FSNamesystem.LOG.debug("Edit log size is now " + fc.size() +
            " written " + written + " bytes " + " at offset " + position);
      }
    }
  }

  /**
   * Returns the file associated with this stream.
   */
  File getFile() {
    return file;
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
}
