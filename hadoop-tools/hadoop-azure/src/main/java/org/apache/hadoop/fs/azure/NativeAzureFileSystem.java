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

package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.net.URI;
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
 * A {@link FileSystem} for reading and writing files stored on <a
 * href="http://store.azure.com/">Windows Azure</a>. This implementation is
 * blob-based and stores files on Azure in their native form so they can be read
 * by other Azure tools.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Deprecated
public class NativeAzureFileSystem extends FileSystem {

  private static final String SCHEME = "wasb";
  private static final String SECURE_SCHEME = "wasbs";

  public static final String WASB_INIT_ERROR_MESSAGE =
      "WASB Driver using wasb(s) schema is no longer supported. "
          + "Instead use ABFS Driver for FNS account by changing the scheme to abfs(s)."
          + "For more details contact askabfs@microsoft.com";

  public NativeAzureFileSystem() {
  }

  public static class Secure extends NativeAzureFileSystem {
    @Override
    public String getScheme() {
      return SECURE_SCHEME;
    }
  }

  /**
   * Fails Any Attempt to use WASB FileSystem Implementation.
   *
   * @param uri  the URI of the file system
   * @param conf the configuration
   * @throws IOException              on IO problems
   * @throws UnsupportedOperationException if the URI is invalid
   */
  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException, UnsupportedOperationException {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public URI getUri() {
    return null;
  }

  @Override
  public FSDataInputStream open(final Path path, final int i)
      throws IOException {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }

  @Override
  public FSDataOutputStream create(final Path path,
      final FsPermission fsPermission,
      final boolean b,
      final int i,
      final short i1,
      final long l,
      final Progressable progressable) throws IOException {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }

  @Override
  public FSDataOutputStream append(final Path path,
      final int i,
      final Progressable progressable)
      throws IOException {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }

  @Override
  public boolean rename(final Path path, final Path path1) throws IOException {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }

  @Override
  public boolean delete(final Path path, final boolean b) throws IOException {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }

  @Override
  public FileStatus[] listStatus(final Path path)
      throws IOException {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }

  @Override
  public void setWorkingDirectory(final Path path) {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }

  @Override
  public Path getWorkingDirectory() {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }

  @Override
  public boolean mkdirs(final Path path, final FsPermission fsPermission)
      throws IOException {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }

  @Override
  public FileStatus getFileStatus(final Path path) throws IOException {
    throw new UnsupportedOperationException(WASB_INIT_ERROR_MESSAGE);
  }
}
