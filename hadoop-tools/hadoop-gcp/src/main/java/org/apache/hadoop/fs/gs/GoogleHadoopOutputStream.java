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

package org.apache.hadoop.fs.gs;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nonnull;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GoogleHadoopOutputStream extends OutputStream {
  public static final Logger LOG = LoggerFactory.getLogger(StorageResourceId.class);

  private final GoogleHadoopFileSystem ghfs;

  // Path of the file to write to.
  private final URI dstGcsPath;

  private OutputStream outputStream;

  // Statistics tracker provided by the parent GoogleHadoopFileSystem for recording
  // numbers of bytes written.
  private final FileSystem.Statistics statistics;

  /**
   * Constructs an instance of GoogleHadoopOutputStream object.
   *
   * @param ghfs              Instance of {@link GoogleHadoopFileSystem}.
   * @param dstGcsPath        Path of the file to write to.
   * @param statistics        File system statistics object.
   * @param createFileOptions options for file creation
   * @throws IOException if an IO error occurs.
   */
  GoogleHadoopOutputStream(GoogleHadoopFileSystem ghfs, URI dstGcsPath,
      CreateOptions createFileOptions, FileSystem.Statistics statistics) throws IOException {
    LOG.trace("GoogleHadoopOutputStream(gcsPath: {}, createFileOptions: {})", dstGcsPath,
        createFileOptions);
    this.ghfs = ghfs;
    this.dstGcsPath = dstGcsPath;
    this.statistics = statistics;

    this.outputStream = createOutputStream(ghfs.getGcsFs(), dstGcsPath, createFileOptions,
        ghfs.getFileSystemConfiguration());
  }

  private static OutputStream createOutputStream(GoogleCloudStorageFileSystem gcsfs, URI gcsPath,
      CreateOptions options, GoogleHadoopFileSystemConfiguration fileSystemConfiguration)
      throws IOException {
    WritableByteChannel channel;
    try {
      channel = gcsfs.create(gcsPath, options);
    } catch (java.nio.file.FileAlreadyExistsException e) {

      throw (FileAlreadyExistsException) new FileAlreadyExistsException(
          String.format("'%s' already exists", gcsPath)).initCause(e);
    }
    OutputStream outputStream = Channels.newOutputStream(channel);
    int bufferSize = fileSystemConfiguration.getOutStreamBufferSize();
    return bufferSize > 0 ? new BufferedOutputStream(outputStream, bufferSize) : outputStream;
  }

  @Override
  public void write(int b) throws IOException {
    throwIfNotOpen();
    outputStream.write(b);
    statistics.incrementBytesWritten(1);
    statistics.incrementWriteOps(1);
  }

  @Override
  public void write(@Nonnull byte[] b, int offset, int len) throws IOException {
    throwIfNotOpen();
    outputStream.write(b, offset, len);
    statistics.incrementBytesWritten(len);
    statistics.incrementWriteOps(1);
  }

  @Override
  public void close() throws IOException {
    LOG.trace("close(): final destination: {}", dstGcsPath);

    if (outputStream == null) {
      LOG.trace("close(): Ignoring; stream already closed.");
      return;
    }

    try {
      outputStream.close();
    } finally {
      outputStream = null;
    }
  }

  private void throwIfNotOpen() throws IOException {
    if (outputStream == null) {
      throw new ClosedChannelException();
    }
  }
}
