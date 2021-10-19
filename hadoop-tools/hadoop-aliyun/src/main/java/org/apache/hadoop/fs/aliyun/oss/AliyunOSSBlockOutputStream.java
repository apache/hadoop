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
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

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
  private boolean closed;
  private String key;
  private File blockFile;
  private Map<Integer, File> blockFiles = new HashMap<>();
  private long blockSize;
  private int blockId = 0;
  private long blockWritten = 0L;
  private String uploadId = null;
  private final List<ListenableFuture<PartETag>> partETagsFutures;
  private final ListeningExecutorService executorService;
  private OutputStream blockStream;
  private final byte[] singleByte = new byte[1];

  public AliyunOSSBlockOutputStream(Configuration conf,
      AliyunOSSFileSystemStore store,
      String key,
      Long blockSize,
      ExecutorService executorService) throws IOException {
    this.store = store;
    this.conf = conf;
    this.key = key;
    this.blockSize = blockSize;
    this.blockFile = newBlockFile();
    this.blockStream =
        new BufferedOutputStream(new FileOutputStream(blockFile));
    this.partETagsFutures = new ArrayList<>(2);
    this.executorService = MoreExecutors.listeningDecorator(executorService);
  }

  private File newBlockFile() throws IOException {
    return AliyunOSSUtils.createTmpFileForWrite(
        String.format("oss-block-%04d-", blockId), blockSize, conf);
  }

  @Override
  public synchronized void flush() throws IOException {
    blockStream.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    blockStream.flush();
    blockStream.close();
    if (!blockFiles.values().contains(blockFile)) {
      blockId++;
      blockFiles.put(blockId, blockFile);
    }

    try {
      if (blockFiles.size() == 1) {
        // just upload it directly
        store.uploadObject(key, blockFile);
      } else {
        if (blockWritten > 0) {
          ListenableFuture<PartETag> partETagFuture =
              executorService.submit(() -> {
                PartETag partETag = store.uploadPart(blockFile, key, uploadId,
                    blockId);
                return partETag;
              });
          partETagsFutures.add(partETagFuture);
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
      removeTemporaryFiles();
      closed = true;
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
    if (closed) {
      throw new IOException("Stream closed.");
    }
    blockStream.write(b, off, len);
    blockWritten += len;
    if (blockWritten >= blockSize) {
      uploadCurrentPart();
      blockWritten = 0L;
    }
  }

  private void removeTemporaryFiles() {
    for (File file : blockFiles.values()) {
      if (file != null && file.exists() && !file.delete()) {
        LOG.warn("Failed to delete temporary file {}", file);
      }
    }
  }

  private void removePartFiles() throws IOException {
    for (ListenableFuture<PartETag> partETagFuture : partETagsFutures) {
      if (!partETagFuture.isDone()) {
        continue;
      }

      try {
        File blockFile = blockFiles.get(partETagFuture.get().getPartNumber());
        if (blockFile != null && blockFile.exists() && !blockFile.delete()) {
          LOG.warn("Failed to delete temporary file {}", blockFile);
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }
  }

  private void uploadCurrentPart() throws IOException {
    blockStream.flush();
    blockStream.close();
    if (blockId == 0) {
      uploadId = store.getUploadId(key);
    }

    blockId++;
    blockFiles.put(blockId, blockFile);

    File currentFile = blockFile;
    int currentBlockId = blockId;
    ListenableFuture<PartETag> partETagFuture =
        executorService.submit(() -> {
          PartETag partETag = store.uploadPart(currentFile, key, uploadId,
              currentBlockId);
          return partETag;
        });
    partETagsFutures.add(partETagFuture);
    removePartFiles();
    blockFile = newBlockFile();
    blockStream = new BufferedOutputStream(new FileOutputStream(blockFile));
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
