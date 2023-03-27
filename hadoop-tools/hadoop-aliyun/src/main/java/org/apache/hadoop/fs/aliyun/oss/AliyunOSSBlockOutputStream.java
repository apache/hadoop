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

package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.model.PartETag;
import org.apache.hadoop.fs.aliyun.oss.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * Asynchronous multi-part based uploading mechanism to support huge file
 * which is larger than 5GB. Data will be buffered on local disk, then uploaded
 * to OSS in {@link #close()} method.
 */
public class AliyunOSSBlockOutputStream extends OutputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(AliyunOSSBlockOutputStream.class);
  private AliyunOSSFileSystemStore store;
  private Configuration conf;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private String key;
  private int blockSize;
  private int blockId = 0;
  private long blockWritten = 0L;
  private String uploadId = null;
  private final List<ListenableFuture<PartETag>> partETagsFutures;
  private final OSSDataBlocks.BlockFactory blockFactory;
  private final BlockOutputStreamStatistics statistics;
  private OSSDataBlocks.DataBlock activeBlock;
  private final ListeningExecutorService executorService;
  private final byte[] singleByte = new byte[1];

  public AliyunOSSBlockOutputStream(Configuration conf,
      AliyunOSSFileSystemStore store,
      String key,
      int blockSize,
      OSSDataBlocks.BlockFactory blockFactory,
      BlockOutputStreamStatistics statistics,
      ExecutorService executorService) throws IOException {
    this.store = store;
    this.conf = conf;
    this.key = key;
    this.blockSize = blockSize;
    this.blockFactory = blockFactory;
    this.statistics = statistics;
    this.partETagsFutures = new ArrayList<>(2);
    this.executorService = MoreExecutors.listeningDecorator(executorService);
  }

  /**
   * Demand create a destination block.
   * @return the active block; null if there isn't one.
   * @throws IOException on any failure to create
   */
  private synchronized OSSDataBlocks.DataBlock createBlockIfNeeded()
      throws IOException {
    if (activeBlock == null) {
      blockId++;
      activeBlock = blockFactory.create(blockId, blockSize, statistics);
    }
    return activeBlock;
  }

  /**
   * Check for the filesystem being open.
   * @throws IOException if the filesystem is closed.
   */
  void checkOpen() throws IOException {
    if (closed.get()) {
      throw new IOException("Stream closed.");
    }
  }

  /**
   * The flush operation does not trigger an upload; that awaits
   * the next block being full. What it does do is call {@code flush() }
   * on the current block, leaving it to choose how to react.
   * @throws IOException Any IO problem.
   */
  @Override
  public synchronized void flush() throws IOException {
    checkOpen();

    OSSDataBlocks.DataBlock dataBlock = getActiveBlock();
    if (dataBlock != null) {
      dataBlock.flush();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed.get()) {
      // already closed
      LOG.debug("Ignoring close() as stream is already closed");
      return;
    }

    try {
      if (uploadId == null) {
        // just upload it directly
        OSSDataBlocks.DataBlock dataBlock = getActiveBlock();
        if (dataBlock == null) {
          // zero size file
          store.storeEmptyFile(key);
        } else {
          OSSDataBlocks.BlockUploadData uploadData = dataBlock.startUpload();
          if (uploadData.hasFile()) {
            store.uploadObject(key, uploadData.getFile());
          } else {
            store.uploadObject(key,
                uploadData.getUploadStream(), dataBlock.dataSize());
          }
        }
      } else {
        if (blockWritten > 0) {
          uploadCurrentBlock();
        }
        // wait for the partial uploads to finish
        final List<PartETag> partETags = waitForAllPartUploads();
        if (null == partETags) {
          throw new IOException("Failed to multipart upload to oss, abort it.");
        }
        store.completeMultipartUpload(key, uploadId,
            new ArrayList<>(partETags));
      }
    } finally {
      cleanupWithLogger(LOG, getActiveBlock(), blockFactory);
      closed.set(true);
    }
  }

  @Override
  public synchronized void write(int b) throws IOException {
    singleByte[0] = (byte)b;
    write(singleByte, 0, 1);
  }

  @Override
  public synchronized void write(byte[] b, int off, int len)
      throws IOException {
    int totalWritten = 0;
    while (totalWritten < len) {
      int written = internalWrite(b, off + totalWritten, len - totalWritten);
      totalWritten += written;
      LOG.debug("Buffer len {}, written {},  total written {}",
          len, written, totalWritten);
    }
  }
  private synchronized int internalWrite(byte[] b, int off, int len)
      throws IOException {
    OSSDataBlocks.validateWriteArgs(b, off, len);
    checkOpen();
    if (len == 0) {
      return 0;
    }
    OSSDataBlocks.DataBlock block = createBlockIfNeeded();
    int written = block.write(b, off, len);
    blockWritten += written;
    int remainingCapacity = block.remainingCapacity();
    if (written < len) {
      // not everything was written — the block has run out
      // of capacity
      // Trigger an upload then process the remainder.
      LOG.debug("writing more data than block has capacity -triggering upload");
      uploadCurrentBlock();
    } else {
      if (remainingCapacity == 0) {
        // the whole buffer is done, trigger an upload
        uploadCurrentBlock();
      }
    }
    return written;
  }

  /**
   * Clear the active block.
   */
  private void clearActiveBlock() {
    if (activeBlock != null) {
      LOG.debug("Clearing active block");
    }
    synchronized (this) {
      activeBlock = null;
    }
  }

  private synchronized OSSDataBlocks.DataBlock getActiveBlock() {
    return activeBlock;
  }

  private void uploadCurrentBlock()
      throws IOException {
    if (uploadId == null) {
      uploadId = store.getUploadId(key);
    }

    int currentBlockId = blockId;
    OSSDataBlocks.DataBlock dataBlock = getActiveBlock();
    long size = dataBlock.dataSize();
    OSSDataBlocks.BlockUploadData uploadData = dataBlock.startUpload();
    try {
      ListenableFuture<PartETag> partETagFuture =
          executorService.submit(() -> {
            try {
              PartETag partETag = store.uploadPart(uploadData, size, key,
                  uploadId, currentBlockId);
              return partETag;
            } finally {
              cleanupWithLogger(LOG, uploadData, dataBlock);
            }
          });
      partETagsFutures.add(partETagFuture);
    } finally {
      blockWritten = 0;
      clearActiveBlock();
    }
  }

  /**
   * Block awaiting all outstanding uploads to complete.
   * @return list of results
   * @throws IOException IO Problems
   */
  private List<PartETag> waitForAllPartUploads() throws IOException {
    LOG.debug("Waiting for {} uploads to complete", partETagsFutures.size());
    try {
      return Futures.allAsList(partETagsFutures).get();
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted partUpload", ie);
      Thread.currentThread().interrupt();
      return null;
    } catch (ExecutionException ee) {
      //there is no way of recovering so abort
      //cancel all partUploads
      LOG.debug("While waiting for upload completion", ee);
      LOG.debug("Cancelling futures");
      for (ListenableFuture<PartETag> future : partETagsFutures) {
        future.cancel(true);
      }
      //abort multipartupload
      store.abortMultipartUpload(key, uploadId);
      throw new IOException("Multi-part upload with id '" + uploadId
        + "' to " + key, ee);
    }
  }
}
