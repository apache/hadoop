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

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.util.Progressable;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;

/**
 * The output stream for OSS blob system.
 * Data will be buffered on local disk, then uploaded to OSS in
 * {@link #close()} method.
 */
public class AliyunOSSOutputStream extends OutputStream {
  public static final Log LOG = LogFactory.getLog(AliyunOSSOutputStream.class);
  private String bucketName;
  private String key;
  private Statistics statistics;
  private Progressable progress;
  private String serverSideEncryptionAlgorithm;
  private long partSize;
  private long partSizeThreshold;
  private LocalDirAllocator dirAlloc;
  private boolean closed;
  private File tmpFile;
  private BufferedOutputStream backupStream;
  private OSSClient ossClient;

  public AliyunOSSOutputStream(Configuration conf, OSSClient client,
      String bucketName, String key, Progressable progress,
      Statistics statistics, String serverSideEncryptionAlgorithm)
      throws IOException {
    this.bucketName = bucketName;
    this.key = key;
    // The caller cann't get any progress information
    this.progress = progress;
    ossClient = client;
    this.statistics = statistics;
    this.serverSideEncryptionAlgorithm = serverSideEncryptionAlgorithm;

    partSize = conf.getLong(MULTIPART_UPLOAD_SIZE_KEY,
        MULTIPART_UPLOAD_SIZE_DEFAULT);
    if (partSize < MIN_MULTIPART_UPLOAD_PART_SIZE) {
      partSize = MIN_MULTIPART_UPLOAD_PART_SIZE;
    }
    partSizeThreshold = conf.getLong(MIN_MULTIPART_UPLOAD_THRESHOLD_KEY,
        MIN_MULTIPART_UPLOAD_THRESHOLD_DEFAULT);

    if (conf.get(BUFFER_DIR_KEY) == null) {
      conf.set(BUFFER_DIR_KEY, conf.get("hadoop.tmp.dir") + "/oss");
    }
    dirAlloc = new LocalDirAllocator(BUFFER_DIR_KEY);

    tmpFile = dirAlloc.createTmpFileForWrite("output-",
        LocalDirAllocator.SIZE_UNKNOWN, conf);
    backupStream = new BufferedOutputStream(new FileOutputStream(tmpFile));
    closed = false;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (backupStream != null) {
      backupStream.close();
    }
    long dataLen = tmpFile.length();
    try {
      if (dataLen <= partSizeThreshold) {
        uploadObject();
      } else {
        multipartUploadObject();
      }
    } finally {
      if (!tmpFile.delete()) {
        LOG.warn("Can not delete file: " + tmpFile);
      }
    }
  }

  /**
   * Upload temporary file as an OSS object, using single upload.
   *
   * @throws IOException
   */
  private void uploadObject() throws IOException {
    File object = tmpFile.getAbsoluteFile();
    FileInputStream fis = new FileInputStream(object);
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(object.length());
    if (!serverSideEncryptionAlgorithm.isEmpty()) {
      meta.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    try {
      PutObjectResult result = ossClient.putObject(bucketName, key, fis, meta);
      LOG.debug(result.getETag());
      statistics.incrementWriteOps(1);
    } finally {
      fis.close();
    }
  }

  /**
   * Upload temporary file as an OSS object, using multipart upload.
   *
   * @throws IOException
   */
  private void multipartUploadObject() throws IOException {
    File object = tmpFile.getAbsoluteFile();
    long dataLen = object.length();
    long realPartSize = AliyunOSSUtils.calculatePartSize(dataLen, partSize);
    int partNum = (int)(dataLen / realPartSize);
    if (dataLen % realPartSize != 0) {
      partNum += 1;
    }

    InitiateMultipartUploadRequest initiateMultipartUploadRequest =
        new InitiateMultipartUploadRequest(bucketName, key);
    ObjectMetadata meta = new ObjectMetadata();
    //    meta.setContentLength(dataLen);
    if (!serverSideEncryptionAlgorithm.isEmpty()) {
      meta.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    initiateMultipartUploadRequest.setObjectMetadata(meta);
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
    List<PartETag> partETags = new ArrayList<PartETag>();
    String uploadId = initiateMultipartUploadResult.getUploadId();

    try {
      for (int i = 0; i < partNum; i++) {
        // TODO: Optimize this, avoid opening the object multiple times
        FileInputStream fis = new FileInputStream(object);
        try {
          long skipBytes = realPartSize * i;
          AliyunOSSUtils.skipFully(fis, skipBytes);
          long size = (realPartSize < dataLen - skipBytes) ?
              realPartSize : dataLen - skipBytes;
          UploadPartRequest uploadPartRequest = new UploadPartRequest();
          uploadPartRequest.setBucketName(bucketName);
          uploadPartRequest.setKey(key);
          uploadPartRequest.setUploadId(uploadId);
          uploadPartRequest.setInputStream(fis);
          uploadPartRequest.setPartSize(size);
          uploadPartRequest.setPartNumber(i + 1);
          UploadPartResult uploadPartResult =
              ossClient.uploadPart(uploadPartRequest);
          statistics.incrementWriteOps(1);
          partETags.add(uploadPartResult.getPartETag());
        } finally {
          fis.close();
        }
      }
      CompleteMultipartUploadRequest completeMultipartUploadRequest =
          new CompleteMultipartUploadRequest(bucketName, key,
          uploadId, partETags);
      CompleteMultipartUploadResult completeMultipartUploadResult =
          ossClient.completeMultipartUpload(completeMultipartUploadRequest);
      LOG.debug(completeMultipartUploadResult.getETag());
    } catch (OSSException | ClientException e) {
      AbortMultipartUploadRequest abortMultipartUploadRequest =
          new AbortMultipartUploadRequest(bucketName, key, uploadId);
      ossClient.abortMultipartUpload(abortMultipartUploadRequest);
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    backupStream.flush();
  }

  @Override
  public synchronized void write(int b) throws IOException {
    backupStream.write(b);
    statistics.incrementBytesWritten(1);
  }

}
