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

package org.apache.hadoop.fs.ozone;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.ozone.web.client.OzoneBucket;
import org.apache.hadoop.ozone.client.rest.OzoneException;

import static org.apache.hadoop.fs.ozone.Constants.BUFFER_DIR_KEY;
import static org.apache.hadoop.fs.ozone.Constants.BUFFER_TMP_KEY;


/**
 * The output stream for Ozone file system.
 *
 * Data will be buffered on local disk, then uploaded to Ozone in
 * {@link #close()} method.
 *
 * This class is not thread safe.
 */
public class OzoneOutputStream extends OutputStream {
  private static final Log LOG = LogFactory.getLog(OzoneOutputStream.class);
  private OzoneBucket bucket;
  private final String key;
  private final URI keyUri;
  private Statistics statistics;
  private LocalDirAllocator dirAlloc;
  private boolean closed;
  private File tmpFile;
  private BufferedOutputStream backupStream;

  OzoneOutputStream(Configuration conf, URI fsUri, OzoneBucket bucket,
      String key, Statistics statistics) throws IOException {
    this.bucket = bucket;
    this.key = key;
    this.keyUri = fsUri.resolve(key);
    this.statistics = statistics;

    if (conf.get(BUFFER_DIR_KEY) == null) {
      conf.set(BUFFER_DIR_KEY, conf.get(BUFFER_TMP_KEY) + "/ozone");
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
    try {
      LOG.trace("Put tmp-file:" + tmpFile + " to key "+ keyUri);
      bucket.putKey(key, tmpFile);
      statistics.incrementWriteOps(1);
    } catch (OzoneException oe) {
      final String msg = "Uploading error: file=" + tmpFile + ", key=" + key;
      LOG.error(msg, oe);
      throw new IOException(msg, oe);
    } finally {
      if (!tmpFile.delete()) {
        LOG.warn("Can not delete tmpFile: " + tmpFile);
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
