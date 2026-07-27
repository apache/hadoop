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
 */

package org.apache.hadoop.fs.bos;

import com.baidubce.services.bos.model.CompleteMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadResponse;
import com.baidubce.services.bos.model.ObjectMetadata;
import com.baidubce.services.bos.model.PartETag;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Output stream implementation for writing data to BOS using
 * multipart uploads. Data is buffered locally and uploaded in
 * parts concurrently.
 */
public class BosOutputStream extends OutputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(BosOutputStream.class);

  private final BosNativeFileSystemStore store;
  private final Map<Integer, BosBlockBuffer> blocksMap =
      new HashMap<>();
  private final String bucketName;
  private final BosClientProxy bosClientProxy;
  private final String key;
  private final Configuration conf;
  private final int blockSize;
  private final int uploadThreadSize;
  private final List<PartETag> eTags =
      Collections.synchronizedList(new ArrayList<>());
  private final List<Future> futureList = new LinkedList<>();
  private final Map<Integer, Exception> exceptionMap =
      new ConcurrentHashMap<>();
  private final ArrayBlockingQueue<BosBlockBuffer> blocks;

  private BosBlockBuffer currBlock;
  private int blkIndex = 1;
  private boolean closed;
  private long filePos = 0;
  private int bytesWrittenToBlock = 0;
  private String uploadId = null;
  private int createBlockSize;

  /**
   * Constructs a BosOutputStream for writing to BOS.
   *
   * @param store      the native file system store
   * @param bucketName the name of the bucket
   * @param key        the object key
   * @param conf       the Hadoop configuration
   */
  public BosOutputStream(
      BosNativeFileSystemStore store,
      String bucketName, String key,
      Configuration conf) {
    this.store = store;
    this.bosClientProxy = store.getBosClientProxy();
    this.bucketName = bucketName;
    this.key = key;
    this.conf = conf;
    this.blockSize = this.conf.getInt(
        BaiduBosConstants.BOS_MULTI_UPLOAD_BLOCK_SIZE,
        BaiduBosConstants
            .BOS_MULTI_UPLOAD_BLOCK_SIZE_DEFAULT);
    this.uploadThreadSize = this.conf.getInt(
        BaiduBosConstants.BOS_MULTI_UPLOAD_CONCURRENT_SIZE,
        BaiduBosConstants
            .DEFAULT_MULTI_UPLOAD_CONCURRENT_SIZE);
    this.blocks =
        new ArrayBlockingQueue<>(uploadThreadSize);
    createBlockSize = 0;
  }

  /**
   * Creates a new block buffer if the current one is null.
   *
   * @throws IOException if allocation fails
   */
  private void createBlockBufferIfNull()
      throws IOException {
    if (this.currBlock == null) {
      LOG.debug(
          "Ready to get Block Buffer !"
              + " key is {}. blkIndex is {}."
              + " blockSize is {}",
          this.key, this.blkIndex, this.blockSize);
      try {
        if (createBlockSize < this.uploadThreadSize) {
          this.currBlock = new BosBlockBuffer(
              this.key, this.blkIndex, blockSize);
          ++createBlockSize;
          LOG.debug(
              "create {}th BosBlockBuffer",
              createBlockSize);
        } else {
          this.currBlock = this.blocks.take();
          this.currBlock.setBlkId(this.blkIndex);
          LOG.debug(
              "get free block: {}", this.currBlock);
        }
        LOG.debug(
            "Block Buffer get !"
                + " key is {}. blkIndex is {}."
                + " blockSize is {}",
            this.key, this.blkIndex,
            this.blockSize);
        this.bytesWrittenToBlock = 0;
        this.blkIndex++;
      } catch (Throwable throwable) {
        LOG.error(
            "catch exception when allocating"
                + " BosBlockBuffer: ",
            throwable);
        throw new IOException(
            "catch exception when allocating"
                + " BosBlockBuffer: ",
            throwable);
      }
    }
  }

  /**
   * Writes a single byte to this output stream.
   *
   * @param b the byte to write
   * @throws IOException if the stream is closed or an I/O
   *                     error occurs
   */
  @Override
  public synchronized void write(int b)
      throws IOException {
    if (this.closed) {
      throw new IOException("Stream closed");
    }

    flush();
    createBlockBufferIfNull();

    this.currBlock.getOutBuffer().write(b);
    this.bytesWrittenToBlock++;
    this.filePos++;
  }

  /**
   * Writes bytes from the specified buffer to this output
   * stream.
   *
   * @param b   the data buffer
   * @param off the start offset in the data
   * @param len the number of bytes to write
   * @throws IOException if the stream is closed or an I/O
   *                     error occurs
   */
  @Override
  public synchronized void write(
      byte[] b, int off, int len) throws IOException {
    if (this.closed) {
      throw new IOException("Stream closed");
    }

    while (len > 0) {
      flush();
      createBlockBufferIfNull();

      int remaining =
          this.blockSize - this.bytesWrittenToBlock;
      int toWrite = Math.min(remaining, len);
      this.currBlock.getOutBuffer().write(b, off, toWrite);
      this.bytesWrittenToBlock += toWrite;
      this.filePos += toWrite;
      off += toWrite;
      len -= toWrite;
    }
  }

  /**
   * Flushes this output stream. If the current block is full,
   * it will be uploaded as a part. Note: flush does not
   * guarantee data durability; it cannot be used for HBase
   * WAL recovery.
   *
   * @throws IOException if the stream is closed or an I/O
   *                     error occurs
   */
  @Override
  public synchronized void flush() throws IOException {
    if (this.closed) {
      throw new IOException("Stream closed");
    }

    if (this.bytesWrittenToBlock >= this.blockSize) {
      endBlock();
    }
  }

  /**
   * Ends the current block by uploading it as a multipart
   * upload part.
   *
   * @throws IOException if an upload error occurs
   */
  private synchronized void endBlock() throws IOException {
    if (this.currBlock == null) {
      return;
    }

    LOG.debug(
        "Enter endBlock() ! key is {}."
            + " blkIndex is {}. blockSize is {}."
            + " Size of eTags is {}."
            + " concurrentUpload is {}",
        this.key, this.blkIndex, this.blockSize,
        this.eTags.size(), this.uploadThreadSize);

    if (this.currBlock.getBlkId() == 1) {
      this.uploadId = initMultipartUpload(this.key);
      LOG.debug(
          "init multipart upload!"
              + " key is {} upload id is {}",
          this.key, this.uploadId);
    }

    //
    // Move outBuffer to inBuffer
    //
    this.currBlock.getOutBuffer().close();
    this.currBlock.moveData();

    //
    // Block this when too many active UploadPartThread
    //
    while ((this.blkIndex - this.eTags.size())
        > this.uploadThreadSize) {
      synchronized (this.blocksMap) {
        try {
          this.blocksMap.wait(10);
        } catch (InterruptedException e) {
          // ignore
        }
      }
      if (this.exceptionMap.size() > 0) {
        //
        // Exception happens during upload
        //
        throw new IOException(
            "Exception happens during upload:"
                + exceptionMap);
      }
    }

    if (this.exceptionMap.size() > 0) {
      //
      // Exception happens during upload
      //
      throw new IOException(
          "Exception happens during upload:"
              + exceptionMap);
    }

    synchronized (this.blocksMap) {
      this.blocksMap.put(
          this.currBlock.getBlkId(), this.currBlock);
    }

    UploadPartThread upThread = new UploadPartThread(
        bosClientProxy,
        bucketName,
        this.key,
        this.currBlock,
        this.uploadId,
        this.eTags,
        this.blocksMap,
        this.exceptionMap,
        this.conf,
        this.blocks
    );
    this.futureList.add(
        store.getBoundedThreadPool().submit(upThread));

    // Clear current BosBlockBuffer
    this.currBlock = null;
  }

  /**
   * Puts the current block as a single object (used when
   * only one block has been written).
   *
   * @throws IOException if the put operation fails
   */
  private synchronized void putObject()
      throws IOException {
    if (this.currBlock == null) {
      return;
    }

    // Move outBuffer to inBuffer
    //
    this.currBlock.getOutBuffer().close();
    this.currBlock.moveData();

    ObjectMetadata objectMeta = new ObjectMetadata();
    objectMeta.setContentLength(
        this.currBlock.getInBuffer().getLength());
    objectMeta.addUserMetadata(
        BaiduBosConstants.BOS_FILE_PATH_USER_KEY,
        store.getEnvUserName());
    objectMeta.addUserMetadata(
        BaiduBosConstants.BOS_FILE_PATH_GROUP_KEY,
        store.getEnvGroupName());
    bosClientProxy.putObject(
        bucketName, this.key,
        this.currBlock.getInBuffer(), objectMeta);
    this.currBlock = null;
  }

  /**
   * Closes this output stream, completing the multipart
   * upload if necessary.
   *
   * @throws IOException if an I/O error occurs during close
   */
  @Override
  public synchronized void close() throws IOException {
    if (this.closed) {
      return;
    }

    if (this.filePos == 0) {
      ObjectMetadata objectMeta = new ObjectMetadata();
      objectMeta.addUserMetadata(
          BaiduBosConstants.BOS_FILE_PATH_USER_KEY,
          store.getEnvUserName());
      objectMeta.addUserMetadata(
          BaiduBosConstants.BOS_FILE_PATH_GROUP_KEY,
          store.getEnvGroupName());
      bosClientProxy.putEmptyObject(
          bucketName, key, objectMeta);
    } else if (null != this.currBlock
        && this.currBlock.getBlkId() == 1) {
      putObject();
    } else {
      if (this.bytesWrittenToBlock != 0) {
        endBlock();
      }

      LOG.debug(
          "start to wait upload part threads done."
              + " futureList size: {}",
          futureList.size());
      // wait UploadPartThread threads done
      Exception uploadException = null;
      try {
        int index = 0;
        for (Future future : this.futureList) {
          future.get();
          index += 1;
          LOG.debug(
              "future.get() index: {} is done",
              index);
        }
      } catch (Exception e) {
        uploadException = e;
        LOG.warn(
            "catch exception when waiting"
                + " UploadPartThread done: ",
            e);
      }

      if (uploadException != null) {
        abortMultipartUpload();
        throw new IOException(
            "Multipart upload failed", uploadException);
      }

      LOG.debug(
          "success to wait upload part threads done");

      LOG.debug(
          "Size of eTags is {}. blkIndex is {}",
          this.eTags.size(), this.blkIndex);

      if (this.eTags.size() != this.blkIndex - 1) {
        abortMultipartUpload();
        throw new IOException(
            "Multipart upload incomplete: expected "
                + (this.blkIndex - 1) + " parts but got "
                + this.eTags.size());
      }

      completeMultipartUpload(
          bucketName, this.key,
          this.uploadId, this.eTags);
    }

    super.close();
    this.closed = true;
  }

  /**
   * Aborts the in-progress multipart upload to prevent
   * storage leakage on failure.
   */
  private void abortMultipartUpload() {
    if (this.uploadId != null) {
      try {
        bosClientProxy.abortMultipartUpload(
            bucketName, this.key, this.uploadId);
      } catch (IOException e) {
        LOG.warn("Failed to abort multipart upload"
            + " for key={}, uploadId={}",
            this.key, this.uploadId, e);
      }
    }
  }

  /**
   * Initiates a multipart upload for the given key.
   *
   * @param objectKey the object key
   * @return the upload ID
   * @throws IOException if the initiation fails
   */
  private String initMultipartUpload(String objectKey)
      throws IOException {
    InitiateMultipartUploadRequest request =
        new InitiateMultipartUploadRequest(
            bucketName, objectKey);

    InitiateMultipartUploadResponse result =
        bosClientProxy.initiateMultipartUpload(request);
    return result.getUploadId();
  }

  /**
   * Completes a multipart upload by assembling previously
   * uploaded parts.
   *
   * @param bucket   the bucket name
   * @param objectKey the object key
   * @param mpUploadId the upload ID
   * @param partETags the list of part ETags
   * @throws IOException if the completion fails
   */
  private void completeMultipartUpload(
      String bucket, String objectKey,
      String mpUploadId, List<PartETag> partETags)
      throws IOException {
    Collections.sort(partETags,
        Comparator.comparingInt(PartETag::getPartNumber));
    ObjectMetadata objectMeta = new ObjectMetadata();
    objectMeta.addUserMetadata(
        BaiduBosConstants.BOS_FILE_PATH_USER_KEY,
        store.getEnvUserName());
    objectMeta.addUserMetadata(
        BaiduBosConstants.BOS_FILE_PATH_GROUP_KEY,
        store.getEnvGroupName());
    CompleteMultipartUploadRequest request =
        new CompleteMultipartUploadRequest(
            bucket, objectKey, mpUploadId,
            partETags, objectMeta);
    bosClientProxy.completeMultipartUpload(request);
  }
}
