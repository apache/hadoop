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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.util.Progressable;

import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;

import static com.amazonaws.event.ProgressEventType.TRANSFER_COMPLETED_EVENT;
import static com.amazonaws.event.ProgressEventType.TRANSFER_PART_STARTED_EVENT;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;

/**
 * Output stream to save data to S3.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AOutputStream extends OutputStream {
  private OutputStream backupStream;
  private File backupFile;
  private boolean closed;
  private String key;
  private String bucket;
  private TransferManager transfers;
  private Progressable progress;
  private long partSize;
  private long partSizeThreshold;
  private S3AFileSystem fs;
  private CannedAccessControlList cannedACL;
  private FileSystem.Statistics statistics;
  private LocalDirAllocator lDirAlloc;
  private String serverSideEncryptionAlgorithm;

  public static final Logger LOG = S3AFileSystem.LOG;

  public S3AOutputStream(Configuration conf, TransferManager transfers,
      S3AFileSystem fs, String bucket, String key, Progressable progress,
      CannedAccessControlList cannedACL, FileSystem.Statistics statistics,
      String serverSideEncryptionAlgorithm)
      throws IOException {
    this.bucket = bucket;
    this.key = key;
    this.transfers = transfers;
    this.progress = progress;
    this.fs = fs;
    this.cannedACL = cannedACL;
    this.statistics = statistics;
    this.serverSideEncryptionAlgorithm = serverSideEncryptionAlgorithm;

    partSize = fs.getPartitionSize();
    partSizeThreshold = fs.getMultiPartThreshold();

    if (conf.get(BUFFER_DIR, null) != null) {
      lDirAlloc = new LocalDirAllocator(BUFFER_DIR);
    } else {
      lDirAlloc = new LocalDirAllocator("${hadoop.tmp.dir}/s3a");
    }

    backupFile = lDirAlloc.createTmpFileForWrite("output-",
        LocalDirAllocator.SIZE_UNKNOWN, conf);
    closed = false;

    LOG.debug("OutputStream for key '{}' writing to tempfile: {}",
        key, backupFile);

    this.backupStream = new BufferedOutputStream(
        new FileOutputStream(backupFile));
  }

  @Override
  public void flush() throws IOException {
    backupStream.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    backupStream.close();
    LOG.debug("OutputStream for key '{}' closed. Now beginning upload", key);
    LOG.debug("Minimum upload part size: {} threshold {}" , partSize,
        partSizeThreshold);


    try {
      final ObjectMetadata om = new ObjectMetadata();
      if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
        om.setSSEAlgorithm(serverSideEncryptionAlgorithm);
      }
      PutObjectRequest putObjectRequest =
          new PutObjectRequest(bucket, key, backupFile);
      putObjectRequest.setCannedAcl(cannedACL);
      putObjectRequest.setMetadata(om);

      Upload upload = transfers.upload(putObjectRequest);

      ProgressableProgressListener listener = 
          new ProgressableProgressListener(upload, progress, statistics);
      upload.addProgressListener(listener);

      upload.waitForUploadResult();

      long delta = upload.getProgress().getBytesTransferred() -
          listener.getLastBytesTransferred();
      if (statistics != null && delta != 0) {
        LOG.debug("S3A write delta changed after finished: {} bytes", delta);
        statistics.incrementBytesWritten(delta);
      }

      // This will delete unnecessary fake parent directories
      fs.finishedWrite(key);
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException(e.toString())
          .initCause(e);
    } catch (AmazonClientException e) {
      throw translateException("saving output", key , e);
    } finally {
      if (!backupFile.delete()) {
        LOG.warn("Could not delete temporary s3a file: {}", backupFile);
      }
      super.close();
      closed = true;
    }
    LOG.debug("OutputStream for key '{}' upload complete", key);
  }

  @Override
  public void write(int b) throws IOException {
    backupStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    backupStream.write(b, off, len);
  }

  /**
   * Listener to progress from AWS regarding transfers.
   */
  public static class ProgressableProgressListener implements ProgressListener {
    private Progressable progress;
    private FileSystem.Statistics statistics;
    private long lastBytesTransferred;
    private Upload upload;

    public ProgressableProgressListener(Upload upload, Progressable progress, 
        FileSystem.Statistics statistics) {
      this.upload = upload;
      this.progress = progress;
      this.statistics = statistics;
      this.lastBytesTransferred = 0;
    }

    public void progressChanged(ProgressEvent progressEvent) {
      if (progress != null) {
        progress.progress();
      }

      // There are 3 http ops here, but this should be close enough for now
      ProgressEventType pet = progressEvent.getEventType();
      if (pet == TRANSFER_PART_STARTED_EVENT ||
          pet == TRANSFER_COMPLETED_EVENT) {
        statistics.incrementWriteOps(1);
      }

      long transferred = upload.getProgress().getBytesTransferred();
      long delta = transferred - lastBytesTransferred;
      if (statistics != null && delta != 0) {
        statistics.incrementBytesWritten(delta);
      }

      lastBytesTransferred = transferred;
    }

    public long getLastBytesTransferred() {
      return lastBytesTransferred;
    }
  }
}
