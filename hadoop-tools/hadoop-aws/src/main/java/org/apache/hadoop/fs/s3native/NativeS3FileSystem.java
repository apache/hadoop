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

package org.apache.hadoop.fs.s3native;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * This is a stub filesystem purely present to fail meaningfully when
 * someone who explicitly declares
 * {@code fs.s3n.impl=org.apache.hadoop.fs.s3native.NativeS3FileSystem}
 * and then tries to create a filesystem off an s3n:// URL.
 *
 * The {@link #initialize(URI, Configuration)} method will throw
 * an IOException informing the user of their need to migrate.
 * @deprecated Replaced by the S3A client.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class NativeS3FileSystem extends FileSystem {
  
  public static final Logger LOG =
      LoggerFactory.getLogger(NativeS3FileSystem.class);

  /**
   * Message in thrown exceptions: {@value}.
   */
  private static final String UNSUPPORTED =
      "The s3n:// client to Amazon S3 is no longer available:"
          + " please migrate to the s3a:// client";

  public NativeS3FileSystem() {
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return <code>s3n</code>
   */
  @Override
  public String getScheme() {
    return "s3n";
  }

  /**
   * Always fail to initialize.
   * @throws IOException always.
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    throw new IOException(UNSUPPORTED);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }

  @Override
  public URI getUri() {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }

  @Override
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }

  @Override
  public FSDataOutputStream append(Path f,
      int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }

  @Override
  public FileStatus[] listStatus(Path f)
      throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }

  @Override
  public Path getWorkingDirectory() {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED);
  }
}
