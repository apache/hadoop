/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import com.qcloud.cos.model.PartETag;

import org.apache.hadoop.conf.Configuration;

/**
 * The output stream for the COS blob store.
 * Implement streaming upload to COS based on the multipart upload function.
 * ( the maximum size of each part is 5GB)
 * Support up to 40TB single file by multipart upload (each part is 5GB).
 * Improve the upload performance of writing large files by using byte buffers
 * and a fixed thread pool.
 */
public class CosNOutputStream extends OutputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(CosNOutputStream.class);

  private final Configuration conf;
  private final NativeFileSystemStore store;
  private MessageDigest digest;
  private long blockSize;
  private String key;
  private int currentBlockId = 0;
  private Set<ByteBufferWrapper> blockCacheBuffers = new HashSet<>();
  private ByteBufferWrapper currentBlockBuffer;
  private OutputStream currentBlockOutputStream;
  private String uploadId = null;
  private ListeningExecutorService executorService;
  private List<ListenableFuture<PartETag>> etagList = new LinkedList<>();
  private int blockWritten = 0;
  private boolean closed = false;

  public CosNOutputStream(Configuration conf, NativeFileSystemStore store,
      String key, long blockSize, ExecutorService executorService)
      throws IOException {
    this.conf = conf;
    this.store = store;
    this.key = key;
    this.blockSize = blockSize;
    if (this.blockSize < Constants.MIN_PART_SIZE) {
      LOG.warn(
          String.format(
              "The minimum size of a single block is limited to %d.",
              Constants.MIN_PART_SIZE));
      this.blockSize = Constants.MIN_PART_SIZE;
    }
    if (this.blockSize > Constants.MAX_PART_SIZE) {
      LOG.warn(
          String.format(
              "The maximum size of a single block is limited to %d.",
              Constants.MAX_PART_SIZE));
      this.blockSize = Constants.MAX_PART_SIZE;
    }

    // Use a blocking thread pool with fair scheduling
    this.executorService = MoreExecutors.listeningDecorator(executorService);

    try {
      this.currentBlockBuffer =
          BufferPool.getInstance().getBuffer((int) this.blockSize);
    } catch (IOException e) {
      throw new IOException("Getting a buffer size: "
          + String.valueOf(this.blockSize)
          + " from buffer pool occurs an exception: ", e);
    }

    try {
      this.digest = MessageDigest.getInstance("MD5");
      this.currentBlockOutputStream = new DigestOutputStream(
          new ByteBufferOutputStream(this.currentBlockBuffer.getByteBuffer()),
          this.digest);
    } catch (NoSuchAlgorithmException e) {
      this.digest = null;
      this.currentBlockOutputStream =
          new ByteBufferOutputStream(this.currentBlockBuffer.getByteBuffer());
    }
  }

  @Override
  public void flush() throws IOException {
    this.currentBlockOutputStream.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    if (this.closed) {
      return;
    }
    this.currentBlockOutputStream.flush();
    this.currentBlockOutputStream.close();
    LOG.info("The output stream has been close, and "
        + "begin to upload the last block: [{}].", this.currentBlockId);
    this.blockCacheBuffers.add(this.currentBlockBuffer);
    if (this.blockCacheBuffers.size() == 1) {
      byte[] md5Hash = this.digest == null ? null : this.digest.digest();
      store.storeFile(this.key,
          new ByteBufferInputStream(this.currentBlockBuffer.getByteBuffer()),
          md5Hash, this.currentBlockBuffer.getByteBuffer().remaining());
    } else {
      PartETag partETag = null;
      if (this.blockWritten > 0) {
        LOG.info("Upload the last part..., blockId: [{}], written bytes: [{}]",
            this.currentBlockId, this.blockWritten);
        partETag = store.uploadPart(
            new ByteBufferInputStream(currentBlockBuffer.getByteBuffer()),
            key, uploadId, currentBlockId + 1,
            currentBlockBuffer.getByteBuffer().remaining());
      }
      final List<PartETag> futurePartETagList = this.waitForFinishPartUploads();
      if (null == futurePartETagList) {
        throw new IOException("Failed to multipart upload to cos, abort it.");
      }
      List<PartETag> tmpPartEtagList = new LinkedList<>(futurePartETagList);
      if (null != partETag) {
        tmpPartEtagList.add(partETag);
      }
      store.completeMultipartUpload(this.key, this.uploadId, tmpPartEtagList);
    }
    try {
      BufferPool.getInstance().returnBuffer(this.currentBlockBuffer);
    } catch (InterruptedException e) {
      LOG.error("An exception occurred "
          + "while returning the buffer to the buffer pool.", e);
    }
    LOG.info("The outputStream for key: [{}] has been uploaded.", key);
    this.blockWritten = 0;
    this.closed = true;
  }

  private List<PartETag> waitForFinishPartUploads() throws IOException {
    try {
      LOG.info("Wait for all parts to finish their uploading.");
      return Futures.allAsList(this.etagList).get();
    } catch (InterruptedException e) {
      LOG.error("Interrupt the part upload.", e);
      return null;
    } catch (ExecutionException e) {
      LOG.error("Cancelling futures.");
      for (ListenableFuture<PartETag> future : this.etagList) {
        future.cancel(true);
      }
      (store).abortMultipartUpload(this.key, this.uploadId);
      LOG.error("Multipart upload with id: [{}] to COS key: [{}]",
          this.uploadId, this.key, e);
      throw new IOException("Multipart upload with id: "
          + this.uploadId + " to " + this.key, e);
    }
  }

  private void uploadPart() throws IOException {
    this.currentBlockOutputStream.flush();
    this.currentBlockOutputStream.close();
    this.blockCacheBuffers.add(this.currentBlockBuffer);

    if (this.currentBlockId == 0) {
      uploadId = (store).getUploadId(key);
    }

    ListenableFuture<PartETag> partETagListenableFuture =
        this.executorService.submit(
            new Callable<PartETag>() {
              private final ByteBufferWrapper buf = currentBlockBuffer;
              private final String localKey = key;
              private final String localUploadId = uploadId;
              private final int blockId = currentBlockId;

              @Override
              public PartETag call() throws Exception {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("{} is uploading a part.",
                      Thread.currentThread().getName());
                }
                PartETag partETag = (store).uploadPart(
                    new ByteBufferInputStream(this.buf.getByteBuffer()),
                    this.localKey, this.localUploadId,
                    this.blockId + 1, this.buf.getByteBuffer().remaining());
                BufferPool.getInstance().returnBuffer(this.buf);
                return partETag;
              }
            });
    this.etagList.add(partETagListenableFuture);
    try {
      this.currentBlockBuffer =
          BufferPool.getInstance().getBuffer((int) this.blockSize);
    } catch (IOException e) {
      String errMsg = String.format("Getting a buffer [size:%d] from "
          + "the buffer pool failed.", this.blockSize);
      throw new IOException(errMsg, e);
    }
    this.currentBlockId++;
    if (null != this.digest) {
      this.digest.reset();
      this.currentBlockOutputStream = new DigestOutputStream(
          new ByteBufferOutputStream(this.currentBlockBuffer.getByteBuffer()),
          this.digest);
    } else {
      this.currentBlockOutputStream =
          new ByteBufferOutputStream(this.currentBlockBuffer.getByteBuffer());
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (this.closed) {
      throw new IOException("block stream has been closed.");
    }

    while (len > 0) {
      long writeBytes;
      if (this.blockWritten + len > this.blockSize) {
        writeBytes = this.blockSize - this.blockWritten;
      } else {
        writeBytes = len;
      }

      this.currentBlockOutputStream.write(b, off, (int) writeBytes);
      this.blockWritten += writeBytes;
      if (this.blockWritten >= this.blockSize) {
        this.uploadPart();
        this.blockWritten = 0;
      }
      len -= writeBytes;
      off += writeBytes;
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    this.write(b, 0, b.length);
  }

  @Override
  public void write(int b) throws IOException {
    if (this.closed) {
      throw new IOException("block stream has been closed.");
    }

    byte[] singleBytes = new byte[1];
    singleBytes[0] = (byte) b;
    this.currentBlockOutputStream.write(singleBytes, 0, 1);
    this.blockWritten += 1;
    if (this.blockWritten >= this.blockSize) {
      this.uploadPart();
      this.blockWritten = 0;
    }
  }
}
