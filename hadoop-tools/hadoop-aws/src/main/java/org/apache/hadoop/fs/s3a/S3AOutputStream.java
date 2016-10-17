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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.util.Progressable;

import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.s3a.S3AUtils.*;

/**
 * Output stream to save data to S3.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AOutputStream extends OutputStream {
  private final OutputStream backupStream;
  private final File backupFile;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final String key;
  private final Progressable progress;
  private final S3AFileSystem fs;

  public static final Logger LOG = S3AFileSystem.LOG;

  public S3AOutputStream(Configuration conf,
      S3AFileSystem fs,
      String key,
      Progressable progress)
      throws IOException {
    this.key = key;
    this.progress = progress;
    this.fs = fs;


    backupFile = fs.createTmpFileForWrite("output-",
        LocalDirAllocator.SIZE_UNKNOWN, conf);

    LOG.debug("OutputStream for key '{}' writing to tempfile: {}",
        key, backupFile);

    this.backupStream = new BufferedOutputStream(
        new FileOutputStream(backupFile));
  }

  /**
   * Check for the filesystem being open.
   * @throws IOException if the filesystem is closed.
   */
  void checkOpen() throws IOException {
    if (closed.get()) {
      throw new IOException("Output Stream closed");
    }
  }

  @Override
  public void flush() throws IOException {
    checkOpen();
    backupStream.flush();
  }

  @Override
  public void close() throws IOException {
    if (closed.getAndSet(true)) {
      return;
    }

    backupStream.close();
    LOG.debug("OutputStream for key '{}' closed. Now beginning upload", key);

    try {
      final ObjectMetadata om = fs.newObjectMetadata(backupFile.length());
      UploadInfo info = fs.putObject(
          fs.newPutObjectRequest(
              key,
              om,
              backupFile));
      ProgressableProgressListener listener =
          new ProgressableProgressListener(fs, key, info.getUpload(), progress);
      info.getUpload().addProgressListener(listener);

      info.getUpload().waitForUploadResult();
      listener.uploadCompleted();
      // This will delete unnecessary fake parent directories, update any
      // MetadataStore
      fs.finishedWrite(key, info.getLength());
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
    }
    LOG.debug("OutputStream for key '{}' upload complete", key);
  }

  @Override
  public void write(int b) throws IOException {
    checkOpen();
    backupStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkOpen();
    backupStream.write(b, off, len);
  }

}
