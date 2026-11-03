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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import com.baidubce.BceServiceException;
import com.baidubce.services.bos.model.PartETag;
import com.baidubce.services.bos.model.UploadPartRequest;
import com.baidubce.services.bos.model.UploadPartResponse;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable that uploads a single part of a multipart upload
 * with configurable retries.
 */
class UploadPartThread implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(UploadPartThread.class);

  private final Configuration conf;
  private final String bucket;
  private final String object;
  private final long size;
  private final List<PartETag> eTags;
  private final int partId;
  private final BosClientProxy client;
  private final String uploadId;
  private final InputStream in;
  private final Map<Integer, BosBlockBuffer> dataMap;
  private final Map<Integer, Exception> exceptionMap;
  private final ArrayBlockingQueue<BosBlockBuffer> blocks;

  /**
   * Construct an upload part thread.
   *
   * @param client the BOS client proxy
   * @param bucket the bucket name
   * @param object the object key
   * @param block the block buffer to upload
   * @param uploadId the multipart upload id
   * @param eTags the list to collect part ETags
   * @param dataMap the block buffer map
   * @param exceptionMap the map to collect exceptions
   * @param conf the configuration
   * @param blocks the queue for recycling block buffers
   */
  UploadPartThread(BosClientProxy client, String bucket,
      String object, BosBlockBuffer block,
      String uploadId, List<PartETag> eTags,
      Map<Integer, BosBlockBuffer> dataMap,
      Map<Integer, Exception> exceptionMap,
      Configuration conf,
      ArrayBlockingQueue<BosBlockBuffer> blocks) {
    this.bucket = bucket;
    this.object = object;
    this.size = block.getInBuffer().getLength();
    this.eTags = eTags;
    this.partId = block.getBlkId();
    this.client = client;
    this.uploadId = uploadId;
    this.in = block.getInBuffer();
    this.dataMap = dataMap;
    this.exceptionMap = exceptionMap;
    this.conf = conf;
    this.blocks = blocks;
  }

  private int getMultiUploadAttempts() {
    return this.conf.getInt(
        BaiduBosConstants.BOS_MAX_RETRIES, 3);
  }

  private int getMultiUploadRetryInterval() {
    return this.conf.getInt(
        BaiduBosConstants.BOS_SLEEP_MILL_SECONDS, 1000);
  }

  @Override
  public void run() {
    String partInfo = object + " with part id " + partId;
    LOG.debug("start uploading {}", partInfo);
    int attempts = this.getMultiUploadAttempts();
    int retryInterval = this.getMultiUploadRetryInterval();
    boolean isSuccess = false;
    UploadPartResponse uploadPartResult = null;
    for (int i = 0; i < attempts; i++) {
      try {
        in.reset();
      } catch (IOException e) {
        LOG.warn("failed to reset input stream for {}",
            partInfo);
        break;
      }

      UploadPartRequest uploadPartRequest =
          new UploadPartRequest();
      uploadPartRequest.setBucketName(bucket);
      uploadPartRequest.setKey(object);
      uploadPartRequest.setUploadId(uploadId);
      uploadPartRequest.setInputStream(in);
      uploadPartRequest.setPartSize(size);
      uploadPartRequest.setPartNumber(partId);
      try {
        LOG.debug("[upload attempt {}] start uploadPart {}",
            i, partInfo);
        uploadPartResult =
            client.uploadPart(uploadPartRequest);
        LOG.debug("[upload attempt {}] end uploadPart {}",
            i, partInfo);
        isSuccess = true;
        break;
      } catch (IOException e) {
        LOG.error("[upload attempt {}] failed to upload {}",
            i, partInfo, e);
        try {
          Thread.sleep(retryInterval);
        } catch (InterruptedException ex) {
          LOG.info("interrupted while sleep");
          Thread.currentThread().interrupt();
        }
      }
    }

    if (!isSuccess) {
      BceServiceException e = new BceServiceException(
          "failed to upload part " + partInfo);
      exceptionMap.put(partId, e);
      LOG.error("upload {} failed after {} attempts",
          partInfo, attempts);
    } else {
      eTags.add(uploadPartResult.getPartETag());
      if (dataMap != null) {
        synchronized (dataMap) {
          dataMap.get(partId).clear();
          dataMap.notifyAll();
        }
      }
      LOG.debug("upload key {} success", partInfo);
    }

    try {
      if (dataMap != null) {
        this.blocks.put(dataMap.get(partId));
      }
      if (in != null) {
        in.close();
      }
    } catch (IOException e) {
      LOG.debug("Error during cleanup for {}", partInfo, e);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted during block recycle for {}",
          partInfo);
      Thread.currentThread().interrupt();
    }
  }
}
