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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.jboss.netty.channel.Channel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * WriteCtx saves the context of one write request, such as request, channel,
 * xid and reply status.
 */
class WriteCtx {
  public static final Log LOG = LogFactory.getLog(WriteCtx.class);
  
  /**
   * In memory write data has 3 states. ALLOW_DUMP: not sequential write, still
   * wait for prerequisite writes. NO_DUMP: sequential write, no need to dump
   * since it will be written to HDFS soon. DUMPED: already dumped to a file.
   */
  public static enum DataState {
    ALLOW_DUMP,
    NO_DUMP,
    DUMPED
  }

  private final FileHandle handle;
  private final long offset;
  private final int count;
  
  /**
   * Some clients can send a write that includes previously written data along
   * with new data. In such case the write request is changed to write from only
   * the new data. {@code originalCount} tracks the number of bytes sent in the
   * request before it was modified to write only the new data. 
   * @see OpenFileCtx#addWritesToCache for more details
   */
  private final int originalCount; 
  public static final int INVALID_ORIGINAL_COUNT = -1;
  
  public int getOriginalCount() {
    return originalCount;
  }

  private final WriteStableHow stableHow;
  private volatile ByteBuffer data;
  
  private final Channel channel;
  private final int xid;
  private boolean replied;

  /** 
   * Data belonging to the same {@link OpenFileCtx} may be dumped to a file. 
   * After being dumped to the file, the corresponding {@link WriteCtx} records 
   * the dump file and the offset.  
   */
  private RandomAccessFile raf;
  private long dumpFileOffset;
  
  private volatile DataState dataState;

  public DataState getDataState() {
    return dataState;
  }

  public void setDataState(DataState dataState) {
    this.dataState = dataState;
  }
  
  /** 
   * Writing the data into a local file. After the writing, if 
   * {@link #dataState} is still ALLOW_DUMP, set {@link #data} to null and set 
   * {@link #dataState} to DUMPED.
   */
  long dumpData(FileOutputStream dumpOut, RandomAccessFile raf)
      throws IOException {
    if (dataState != DataState.ALLOW_DUMP) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No need to dump with status(replied,dataState):" + "("
            + replied + "," + dataState + ")");
      }
      return 0;
    }

    // Resized write should not allow dump
    Preconditions.checkState(originalCount == INVALID_ORIGINAL_COUNT);

    this.raf = raf;
    dumpFileOffset = dumpOut.getChannel().position();
    dumpOut.write(data.array(), 0, count);
    if (LOG.isDebugEnabled()) {
      LOG.debug("After dump, new dumpFileOffset:" + dumpFileOffset);
    }
    // it is possible that while we dump the data, the data is also being
    // written back to HDFS. After dump, if the writing back has not finished
    // yet, we change its flag to DUMPED and set the data to null. Otherwise
    // this WriteCtx instance should have been removed from the buffer.
    if (dataState == DataState.ALLOW_DUMP) {
      synchronized (this) {
        if (dataState == DataState.ALLOW_DUMP) {
          data = null;
          dataState = DataState.DUMPED;
          return count;
        }
      }
    }
    return 0;
  }

  FileHandle getHandle() {
    return handle;
  }
  
  long getOffset() {
    return offset;
  }

  int getCount() {
    return count;
  }

  WriteStableHow getStableHow() {
    return stableHow;
  }

  @VisibleForTesting
  ByteBuffer getData() throws IOException {
    if (dataState != DataState.DUMPED) {
      synchronized (this) {
        if (dataState != DataState.DUMPED) {
          Preconditions.checkState(data != null);
          return data;
        }
      }
    }
    // read back from dumped file
    this.loadData();
    return data;
  }

  private void loadData() throws IOException {
    Preconditions.checkState(data == null);
    byte[] rawData = new byte[count];
    raf.seek(dumpFileOffset);
    int size = raf.read(rawData, 0, count);
    if (size != count) {
      throw new IOException("Data count is " + count + ", but read back "
          + size + "bytes");
    }
    data = ByteBuffer.wrap(rawData);
  }

  public void writeData(HdfsDataOutputStream fos) throws IOException {
    Preconditions.checkState(fos != null);

    ByteBuffer dataBuffer;
    try {
      dataBuffer = getData();
    } catch (Exception e1) {
      LOG.error("Failed to get request data offset:" + offset + " count:"
          + count + " error:" + e1);
      throw new IOException("Can't get WriteCtx.data");
    }

    byte[] data = dataBuffer.array();
    int position = dataBuffer.position();
    int limit = dataBuffer.limit();
    Preconditions.checkState(limit - position == count);
    // Modified write has a valid original count
    if (position != 0) {
      if (limit != getOriginalCount()) {
        throw new IOException("Modified write has differnt original size."
            + "buff position:" + position + " buff limit:" + limit + ". "
            + toString());
      }
    }
    
    // Now write data
    fos.write(data, position, count);
  }
  
  Channel getChannel() {
    return channel;
  }

  int getXid() {
    return xid;
  }

  boolean getReplied() {
    return replied;
  }
  
  void setReplied(boolean replied) {
    this.replied = replied;
  }
  
  WriteCtx(FileHandle handle, long offset, int count, int originalCount,
      WriteStableHow stableHow, ByteBuffer data, Channel channel, int xid,
      boolean replied, DataState dataState) {
    this.handle = handle;
    this.offset = offset;
    this.count = count;
    this.originalCount = originalCount;
    this.stableHow = stableHow;
    this.data = data;
    this.channel = channel;
    this.xid = xid;
    this.replied = replied;
    this.dataState = dataState;
    raf = null;
  }
  
  @Override
  public String toString() {
    return "Id:" + handle.getFileId() + " offset:" + offset + " count:" + count
        + " originalCount:" + originalCount + " stableHow:" + stableHow
        + " replied:" + replied + " dataState:" + dataState + " xid:" + xid;
  }
}