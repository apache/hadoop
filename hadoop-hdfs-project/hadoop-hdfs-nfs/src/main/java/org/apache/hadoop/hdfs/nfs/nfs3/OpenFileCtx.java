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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.InvalidParameterException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.nfs.nfs3.WriteCtx.DataState;
import org.apache.hadoop.io.BytesWritable.Comparator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.IdUserGroup;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.nfs.nfs3.response.WccData;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.jboss.netty.channel.Channel;

/**
 * OpenFileCtx saves the context of one HDFS file output stream. Access to it is
 * synchronized by its member lock.
 */
class OpenFileCtx {
  public static final Log LOG = LogFactory.getLog(OpenFileCtx.class);
  
  /**
   * Lock to synchronize OpenFileCtx changes. Thread should get this lock before
   * any read/write operation to an OpenFileCtx object
   */
  private final ReentrantLock ctxLock;

  // The stream status. False means the stream is closed.
  private boolean activeState;
  // The stream write-back status. True means one thread is doing write back.
  private boolean asyncStatus;

  private final HdfsDataOutputStream fos;
  private final Nfs3FileAttributes latestAttr;
  private long nextOffset;

  private final SortedMap<OffsetRange, WriteCtx> pendingWrites;
  
  // The last write, commit request or write-back event. Updating time to keep
  // output steam alive.
  private long lastAccessTime;
  
  // Pending writes water mark for dump, 1MB
  private static int DUMP_WRITE_WATER_MARK = 1024 * 1024; 
  private FileOutputStream dumpOut;
  private long nonSequentialWriteInMemory;
  private boolean enabledDump;
  private RandomAccessFile raf;
  private final String dumpFilePath;
  
  private void updateLastAccessTime() {
    lastAccessTime = System.currentTimeMillis();
  }

  private boolean checkStreamTimeout(long streamTimeout) {
    return System.currentTimeMillis() - lastAccessTime > streamTimeout;
  }
  
  // Increase or decrease the memory occupation of non-sequential writes
  private long updateNonSequentialWriteInMemory(long count) {
    nonSequentialWriteInMemory += count;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Update nonSequentialWriteInMemory by " + count + " new value:"
          + nonSequentialWriteInMemory);
    }

    if (nonSequentialWriteInMemory < 0) {
      LOG.error("nonSequentialWriteInMemory is negative after update with count "
          + count);
      throw new IllegalArgumentException(
          "nonSequentialWriteInMemory is negative after update with count "
              + count);
    }
    return nonSequentialWriteInMemory;
  }
  
  OpenFileCtx(HdfsDataOutputStream fos, Nfs3FileAttributes latestAttr,
      String dumpFilePath) {
    this.fos = fos;
    this.latestAttr = latestAttr;
    pendingWrites = new TreeMap<OffsetRange, WriteCtx>();
    updateLastAccessTime();
    activeState = true;
    asyncStatus = false;
    dumpOut = null;
    raf = null;
    nonSequentialWriteInMemory = 0;
    this.dumpFilePath = dumpFilePath;  
    enabledDump = dumpFilePath == null ? false: true;
    nextOffset = latestAttr.getSize();
    assert(nextOffset == this.fos.getPos());

    ctxLock = new ReentrantLock(true);
  }

  private void lockCtx() {
    if (LOG.isTraceEnabled()) {
      StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
      StackTraceElement e = stacktrace[2];
      String methodName = e.getMethodName();
      LOG.trace("lock ctx, caller:" + methodName);
    }
    ctxLock.lock();
  }

  private void unlockCtx() {
    ctxLock.unlock();
    if (LOG.isTraceEnabled()) {
      StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
      StackTraceElement e = stacktrace[2];
      String methodName = e.getMethodName();
      LOG.info("unlock ctx, caller:" + methodName);
    }
  }
  
  // Make a copy of the latestAttr
  public Nfs3FileAttributes copyLatestAttr() {
    Nfs3FileAttributes ret;
    lockCtx();
    try {
      ret = new Nfs3FileAttributes(latestAttr);
    } finally {
      unlockCtx();
    }
    return ret;
  }
  
  private long getNextOffsetUnprotected() {
    assert(ctxLock.isLocked());
    return nextOffset;
  }

  public long getNextOffset() {
    long ret;
    lockCtx();
    try {
      ret = getNextOffsetUnprotected();
    } finally {
      unlockCtx();
    }
    return ret;
  }
  
  // Get flushed offset. Note that flushed data may not be persisted.
  private long getFlushedOffset() {
    return fos.getPos();
  }
  
  // Check if need to dump the new writes
  private void checkDump(long count) {
    assert (ctxLock.isLocked());

    // Always update the in memory count
    updateNonSequentialWriteInMemory(count);

    if (!enabledDump) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Do nothing, dump is disabled.");
      }
      return;
    }

    if (nonSequentialWriteInMemory < DUMP_WRITE_WATER_MARK) {
      return;
    }

    // Create dump outputstream for the first time
    if (dumpOut == null) {
      LOG.info("Create dump file:" + dumpFilePath);
      File dumpFile = new File(dumpFilePath);
      try {
        if (dumpFile.exists()) {
          LOG.fatal("The dump file should not exist:" + dumpFilePath);
          throw new RuntimeException("The dump file should not exist:"
              + dumpFilePath);
        }
        dumpOut = new FileOutputStream(dumpFile);
      } catch (IOException e) {
        LOG.error("Got failure when creating dump stream " + dumpFilePath
            + " with error:" + e);
        enabledDump = false;
        IOUtils.cleanup(LOG, dumpOut);
        return;
      }
    }
    // Get raf for the first dump
    if (raf == null) {
      try {
        raf = new RandomAccessFile(dumpFilePath, "r");
      } catch (FileNotFoundException e) {
        LOG.error("Can't get random access to file " + dumpFilePath);
        // Disable dump
        enabledDump = false;
        return;
      }
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Start dump, current write number:" + pendingWrites.size());
    }
    Iterator<OffsetRange> it = pendingWrites.keySet().iterator();
    while (it.hasNext()) {
      OffsetRange key = it.next();
      WriteCtx writeCtx = pendingWrites.get(key);
      try {
        long dumpedDataSize = writeCtx.dumpData(dumpOut, raf);
        if (dumpedDataSize > 0) {
          updateNonSequentialWriteInMemory(-dumpedDataSize);
        }
      } catch (IOException e) {
        LOG.error("Dump data failed:" + writeCtx + " with error:" + e);
        // Disable dump
        enabledDump = false;
        return;
      }
    }
    if (nonSequentialWriteInMemory != 0) {
      LOG.fatal("After dump, nonSequentialWriteInMemory is not zero: "
          + nonSequentialWriteInMemory);
      throw new RuntimeException(
          "After dump, nonSequentialWriteInMemory is not zero: "
              + nonSequentialWriteInMemory);
    }
  }
  
  private WriteCtx checkRepeatedWriteRequest(WRITE3Request request,
      Channel channel, int xid) {
    OffsetRange range = new OffsetRange(request.getOffset(),
        request.getOffset() + request.getCount());
    WriteCtx writeCtx = pendingWrites.get(range);
    if (writeCtx== null) {
      return null;
    } else {
      if (xid != writeCtx.getXid()) {
        LOG.warn("Got a repeated request, same range, with a different xid:"
            + xid + " xid in old request:" + writeCtx.getXid());
        //TODO: better handling.
      }
      return writeCtx;  
    }
  }
  
  public void receivedNewWrite(DFSClient dfsClient, WRITE3Request request,
      Channel channel, int xid, AsyncDataService asyncDataService,
      IdUserGroup iug) {

    lockCtx();
    try {
      if (!activeState) {
        LOG.info("OpenFileCtx is inactive, fileId:"
            + request.getHandle().getFileId());
        WccData fileWcc = new WccData(latestAttr.getWccAttr(), latestAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO,
            fileWcc, 0, request.getStableHow(), Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.writeHeaderAndResponse(
            new XDR(), xid, new VerifierNone()), xid);
      } else {
        // Handle repeated write requests(same xid or not).
        // If already replied, send reply again. If not replied, drop the
        // repeated request.
        WriteCtx existantWriteCtx = checkRepeatedWriteRequest(request, channel,
            xid);
        if (existantWriteCtx != null) {
          if (!existantWriteCtx.getReplied()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Repeated write request which hasn't be served: xid="
                  + xid + ", drop it.");
            }
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Repeated write request which is already served: xid="
                  + xid + ", resend response.");
            }
            WccData fileWcc = new WccData(latestAttr.getWccAttr(), latestAttr);
            WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
                fileWcc, request.getCount(), request.getStableHow(),
                Nfs3Constant.WRITE_COMMIT_VERF);
            Nfs3Utils.writeChannel(channel, response.writeHeaderAndResponse(
                new XDR(), xid, new VerifierNone()), xid);
          }
          updateLastAccessTime();
          
        } else {
          receivedNewWriteInternal(dfsClient, request, channel, xid,
              asyncDataService, iug);
        }
      }

    } finally {
      unlockCtx();
    }
  }

  private void receivedNewWriteInternal(DFSClient dfsClient,
      WRITE3Request request, Channel channel, int xid,
      AsyncDataService asyncDataService, IdUserGroup iug) {
    long offset = request.getOffset();
    int count = request.getCount();
    WriteStableHow stableHow = request.getStableHow();

    // Get file length, fail non-append call
    WccAttr preOpAttr = latestAttr.getWccAttr();
    if (LOG.isDebugEnabled()) {
      LOG.debug("requesed offset=" + offset + " and current filesize="
          + preOpAttr.getSize());
    }

    long nextOffset = getNextOffsetUnprotected();
    if (offset == nextOffset) {
      LOG.info("Add to the list, update nextOffset and notify the writer,"
          + " nextOffset:" + nextOffset);
      WriteCtx writeCtx = new WriteCtx(request.getHandle(),
          request.getOffset(), request.getCount(), request.getStableHow(),
          request.getData().array(), channel, xid, false, DataState.NO_DUMP);
      addWrite(writeCtx);
      
      // Create an async task and change openFileCtx status to indicate async
      // task pending
      if (!asyncStatus) {
        asyncStatus = true;
        asyncDataService.execute(new AsyncDataService.WriteBackTask(this));
      }
      
      // Update the write time first
      updateLastAccessTime();
      Nfs3FileAttributes postOpAttr = new Nfs3FileAttributes(latestAttr);

      // Send response immediately for unstable write
      if (request.getStableHow() == WriteStableHow.UNSTABLE) {
        WccData fileWcc = new WccData(preOpAttr, postOpAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
            fileWcc, count, stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.writeHeaderAndResponse(
            new XDR(), xid, new VerifierNone()), xid);
        writeCtx.setReplied(true);
      }

    } else if (offset > nextOffset) {
      LOG.info("Add new write to the list but not update nextOffset:"
          + nextOffset);
      WriteCtx writeCtx = new WriteCtx(request.getHandle(),
          request.getOffset(), request.getCount(), request.getStableHow(),
          request.getData().array(), channel, xid, false, DataState.ALLOW_DUMP);
      addWrite(writeCtx);

      // Check if need to dump some pending requests to file
      checkDump(request.getCount());
      updateLastAccessTime();
      Nfs3FileAttributes postOpAttr = new Nfs3FileAttributes(latestAttr);
      
      // In test, noticed some Linux client sends a batch (e.g., 1MB)
      // of reordered writes and won't send more writes until it gets
      // responses of the previous batch. So here send response immediately for
      // unstable non-sequential write
      if (request.getStableHow() == WriteStableHow.UNSTABLE) {
        WccData fileWcc = new WccData(preOpAttr, postOpAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
            fileWcc, count, stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.writeHeaderAndResponse(
            new XDR(), xid, new VerifierNone()), xid);
        writeCtx.setReplied(true);
      }

    } else {
      // offset < nextOffset
      LOG.warn("(offset,count,nextOffset):" + "(" + offset + "," + count + ","
          + nextOffset + ")");
      WccData wccData = new WccData(preOpAttr, null);
      WRITE3Response response;

      if (offset + count > nextOffset) {
        LOG.warn("Haven't noticed any partial overwrite out of a sequential file"
            + "write requests, so treat it as a real random write, no support.");
        response = new WRITE3Response(Nfs3Status.NFS3ERR_INVAL, wccData, 0,
            WriteStableHow.UNSTABLE, 0);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Process perfectOverWrite");
        }
        response = processPerfectOverWrite(dfsClient, offset, count, stableHow,
            request.getData().array(),
            Nfs3Utils.getFileIdPath(request.getHandle()), wccData, iug);
      }
      
      updateLastAccessTime();
      Nfs3Utils.writeChannel(channel, response.writeHeaderAndResponse(
          new XDR(), xid, new VerifierNone()), xid);
    }
  }
  
  /**
   * Honor 2 kinds of overwrites: 1). support some application like touch(write
   * the same content back to change mtime), 2) client somehow sends the same
   * write again in a different RPC.
   */
  private WRITE3Response processPerfectOverWrite(DFSClient dfsClient,
      long offset, int count, WriteStableHow stableHow, byte[] data,
      String path, WccData wccData, IdUserGroup iug) {
    assert (ctxLock.isLocked());
    WRITE3Response response = null;

    // Read the content back
    byte[] readbuffer = new byte[count];

    int readCount = 0;
    FSDataInputStream fis = null;
    try {
      // Sync file data and length to avoid partial read failure
      fos.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
      
      fis = new FSDataInputStream(dfsClient.open(path));
      readCount = fis.read(offset, readbuffer, 0, count);
      if (readCount < count) {
        LOG.error("Can't read back " + count + " bytes, partial read size:"
            + readCount);
        return response = new WRITE3Response(Nfs3Status.NFS3ERR_IO, wccData, 0,
            stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
      }

    } catch (IOException e) {
      LOG.info("Read failed when processing possible perfect overwrite, path="
          + path + " error:" + e);
      return response = new WRITE3Response(Nfs3Status.NFS3ERR_IO, wccData, 0,
          stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
    } finally {
      IOUtils.cleanup(LOG, fis);
    }

    // Compare with the request
    Comparator comparator = new Comparator();
    if (comparator.compare(readbuffer, 0, readCount, data, 0, count) != 0) {
      LOG.info("Perfect overwrite has different content");
      response = new WRITE3Response(Nfs3Status.NFS3ERR_INVAL, wccData, 0,
          stableHow, 0);
    } else {
      LOG.info("Perfect overwrite has same content,"
          + " updating the mtime, then return success");
      Nfs3FileAttributes postOpAttr = null;
      try {
        dfsClient.setTimes(path, System.currentTimeMillis(), -1);
        postOpAttr = Nfs3Utils.getFileAttr(dfsClient, path, iug);
      } catch (IOException e) {
        LOG.info("Got error when processing perfect overwrite, path=" + path
            + " error:" + e);
        return new WRITE3Response(Nfs3Status.NFS3ERR_IO, wccData, 0, stableHow,
            0);
      }

      wccData.setPostOpAttr(postOpAttr);
      response = new WRITE3Response(Nfs3Status.NFS3_OK, wccData, count,
          stableHow, 0);
    }
    return response;
  }
  
  public final static int COMMIT_FINISHED = 0;
  public final static int COMMIT_WAIT = 1;
  public final static int COMMIT_INACTIVE_CTX = 2;
  public final static int COMMIT_ERROR = 3;

  /**
   * return one commit status: COMMIT_FINISHED, COMMIT_WAIT,
   * COMMIT_INACTIVE_CTX, COMMIT_ERROR
   */
  public int checkCommit(long commitOffset) {
    int ret = COMMIT_WAIT;

    lockCtx();
    try {
      if (!activeState) {
        ret = COMMIT_INACTIVE_CTX;
      } else {
        ret = checkCommitInternal(commitOffset);
      }
    } finally {
      unlockCtx();
    }
    return ret;
  }
  
  private int checkCommitInternal(long commitOffset) {
    if (commitOffset == 0) {
      // Commit whole file
      commitOffset = getNextOffsetUnprotected();
    }

    long flushed = getFlushedOffset();
    LOG.info("getFlushedOffset=" + flushed + " commitOffset=" + commitOffset);
    if (flushed < commitOffset) {
      // Keep stream active
      updateLastAccessTime();
      return COMMIT_WAIT;
    }

    int ret = COMMIT_WAIT;
    try {
      // Sync file data and length
      fos.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
      // Nothing to do for metadata since attr related change is pass-through
      ret = COMMIT_FINISHED;
    } catch (IOException e) {
      LOG.error("Got stream error during data sync:" + e);
      // Do nothing. Stream will be closed eventually by StreamMonitor.
      ret = COMMIT_ERROR;
    }

    // Keep stream active
    updateLastAccessTime();
    return ret;
  }
  
  private void addWrite(WriteCtx writeCtx) {
    assert (ctxLock.isLocked());
    long offset = writeCtx.getOffset();
    int count = writeCtx.getCount();
    pendingWrites.put(new OffsetRange(offset, offset + count), writeCtx);
  }
  
  
  /**
   * Check stream status to decide if it should be closed
   * @return true, remove stream; false, keep stream
   */
  public boolean streamCleanup(long fileId, long streamTimeout) {
    if (streamTimeout < WriteManager.MINIMIUM_STREAM_TIMEOUT) {
      throw new InvalidParameterException("StreamTimeout" + streamTimeout
          + "ms is less than MINIMIUM_STREAM_TIMEOUT "
          + WriteManager.MINIMIUM_STREAM_TIMEOUT + "ms");
    }
    
    boolean flag = false;
    if (!ctxLock.tryLock()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Another thread is working on it" + ctxLock.toString());
      }
      return flag;
    }
    
    try {
      // Check the stream timeout
      if (checkStreamTimeout(streamTimeout)) {
        LOG.info("closing stream for fileId:" + fileId);
        cleanup();
        flag = true;
      }
    } finally {
      unlockCtx();
    }
    return flag;
  }
  
  // Invoked by AsynDataService to do the write back
  public void executeWriteBack() {
    long nextOffset;
    OffsetRange key;
    WriteCtx writeCtx;

    try {
      // Don't lock OpenFileCtx for all writes to reduce the timeout of other
      // client request to the same file
      while (true) {
        lockCtx();
        if (!asyncStatus) {
          // This should never happen. There should be only one thread working
          // on one OpenFileCtx anytime.
          LOG.fatal("The openFileCtx has false async status");
          throw new RuntimeException("The openFileCtx has false async status");
        }
        // Any single write failure can change activeState to false, so do the
        // check each loop.
        if (pendingWrites.isEmpty()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("The asyn write task has no pendding writes, fileId: "
                + latestAttr.getFileId());
          }
          break;
        }
        if (!activeState) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("The openFileCtx is not active anymore, fileId: "
                + latestAttr.getFileId());
          }
          break;
        }

        // Get the next sequential write
        nextOffset = getNextOffsetUnprotected();
        key = pendingWrites.firstKey();
        if (LOG.isTraceEnabled()) {
          LOG.trace("key.getMin()=" + key.getMin() + " nextOffset="
              + nextOffset);
        }

        if (key.getMin() > nextOffset) {
          if (LOG.isDebugEnabled()) {
            LOG.info("The next sequencial write has not arrived yet");
          }
          break;

        } else if (key.getMin() < nextOffset && key.getMax() > nextOffset) {
          // Can't handle overlapping write. Didn't see it in tests yet.
          LOG.fatal("Got a overlapping write (" + key.getMin() + ","
              + key.getMax() + "), nextOffset=" + nextOffset);
          throw new RuntimeException("Got a overlapping write (" + key.getMin()
              + "," + key.getMax() + "), nextOffset=" + nextOffset);

        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Remove write(" + key.getMin() + "-" + key.getMax()
                + ") from the list");
          }
          writeCtx = pendingWrites.remove(key);
          // Do the write
          doSingleWrite(writeCtx);
          updateLastAccessTime();
        }
        
        unlockCtx();
      }

    } finally {
      // Always reset the async status so another async task can be created
      // for this file
      asyncStatus = false;
      if (ctxLock.isHeldByCurrentThread()) {
        unlockCtx();
      }
    }
  }

  private void doSingleWrite(final WriteCtx writeCtx) {
    assert(ctxLock.isLocked());
    Channel channel = writeCtx.getChannel();
    int xid = writeCtx.getXid();

    long offset = writeCtx.getOffset();
    int count = writeCtx.getCount();
    WriteStableHow stableHow = writeCtx.getStableHow();
    byte[] data = null;
    try {
      data = writeCtx.getData();
    } catch (IOException e1) {
      LOG.error("Failed to get request data offset:" + offset + " count:"
          + count + " error:" + e1);
      // Cleanup everything
      cleanup();
      return;
    }
    assert (data.length == count);

    FileHandle handle = writeCtx.getHandle();
    LOG.info("do write, fileId: " + handle.getFileId() + " offset: " + offset
        + " length:" + count + " stableHow:" + stableHow.getValue());

    try {
      fos.write(data, 0, count);
      
      long flushedOffset = getFlushedOffset();
      if (flushedOffset != (offset + count)) {
        throw new IOException("output stream is out of sync, pos="
            + flushedOffset + " and nextOffset should be"
            + (offset + count));
      }
      nextOffset = flushedOffset;

      // Reduce memory occupation size if request was allowed dumped
      if (writeCtx.getDataState() == DataState.ALLOW_DUMP) {
        updateNonSequentialWriteInMemory(-count);
      }
      
      if (!writeCtx.getReplied()) {
        WccAttr preOpAttr = latestAttr.getWccAttr();
        WccData fileWcc = new WccData(preOpAttr, latestAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
            fileWcc, count, stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.writeHeaderAndResponse(
            new XDR(), xid, new VerifierNone()), xid);
      }

    } catch (IOException e) {
      LOG.error("Error writing to fileId " + handle.getFileId() + " at offset "
          + offset + " and length " + data.length, e);
      if (!writeCtx.getReplied()) {
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO);
        Nfs3Utils.writeChannel(channel, response.writeHeaderAndResponse(
            new XDR(), xid, new VerifierNone()), xid);
        // Keep stream open. Either client retries or SteamMonitor closes it.
      }

      LOG.info("Clean up open file context for fileId: "
          + latestAttr.getFileid());
      cleanup();
    }
  }

  private void cleanup() {
    assert(ctxLock.isLocked());
    activeState = false;
    
    // Close stream
    try {
      if (fos != null) {
        fos.close();
      }
    } catch (IOException e) {
      LOG.info("Can't close stream for fileId:" + latestAttr.getFileid()
          + ", error:" + e);
    }
    
    // Reply error for pending writes
    LOG.info("There are " + pendingWrites.size() + " pending writes.");
    WccAttr preOpAttr = latestAttr.getWccAttr();
    while (!pendingWrites.isEmpty()) {
      OffsetRange key = pendingWrites.firstKey();
      LOG.info("Fail pending write: (" + key.getMin() + "," + key.getMax()
          + "), nextOffset=" + getNextOffsetUnprotected());
      
      WriteCtx writeCtx = pendingWrites.remove(key);
      if (!writeCtx.getReplied()) {
        WccData fileWcc = new WccData(preOpAttr, latestAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO,
            fileWcc, 0, writeCtx.getStableHow(), Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(writeCtx.getChannel(), response
            .writeHeaderAndResponse(new XDR(), writeCtx.getXid(),
                new VerifierNone()), writeCtx.getXid());
      }
    }
    
    // Cleanup dump file
    if (dumpOut!=null){
      try {
        dumpOut.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (raf!=null) {
      try {
        raf.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    File dumpFile = new File(dumpFilePath);
    if (dumpFile.delete()) {
      LOG.error("Failed to delete dumpfile: "+ dumpFile);
    }
  }
}