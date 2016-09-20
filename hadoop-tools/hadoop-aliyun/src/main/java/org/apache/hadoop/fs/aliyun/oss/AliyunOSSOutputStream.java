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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

/**
 * The output stream for OSS blob system.
 * Data will be buffered on local disk, then uploaded to OSS in
 * {@link #close()} method.
 */
public class AliyunOSSOutputStream extends OutputStream {
  public static final Log LOG = LogFactory.getLog(AliyunOSSOutputStream.class);
  private AliyunOSSFileSystemStore store;
  private final String key;
  private Statistics statistics;
  private Progressable progress;
  private long partSizeThreshold;
  private LocalDirAllocator dirAlloc;
  private boolean closed;
  private File tmpFile;
  private BufferedOutputStream backupStream;

  public AliyunOSSOutputStream(Configuration conf,
      AliyunOSSFileSystemStore store, String key, Progressable progress,
      Statistics statistics) throws IOException {
    this.store = store;
    this.key = key;
    // The caller cann't get any progress information
    this.progress = progress;
    this.statistics = statistics;
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
        store.uploadObject(key, tmpFile);
      } else {
        store.multipartUploadObject(key, tmpFile);
      }
    } finally {
      if (!tmpFile.delete()) {
        LOG.warn("Can not delete file: " + tmpFile);
      }
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
