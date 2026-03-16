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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Adapter that bridges {@link BaiduBosFileSystem} to the
 * Hadoop {@link DelegateToFileSystem} abstraction for the
 * "bos" URI scheme.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BaiduBosFileSystemAdapter
    extends DelegateToFileSystem {

  /**
   * Constructs a BaiduBosFileSystemAdapter.
   *
   * @param theUri the URI for this filesystem
   * @param conf   the Hadoop configuration
   * @throws IOException        if an I/O error occurs
   * @throws URISyntaxException if the URI is malformed
   */
  public BaiduBosFileSystemAdapter(
      URI theUri, Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, new BaiduBosFileSystem(),
        conf, "bos", false);
  }

  /**
   * Constructs a BaiduBosFileSystemAdapter with an explicit
   * filesystem implementation.
   *
   * @param theUri          the URI for this filesystem
   * @param theFsImpl       the filesystem implementation
   * @param conf            the Hadoop configuration
   * @param supportedScheme the supported URI scheme
   * @param authorityRequired whether authority is required
   * @throws IOException        if an I/O error occurs
   * @throws URISyntaxException if the URI is malformed
   */
  protected BaiduBosFileSystemAdapter(
      URI theUri, FileSystem theFsImpl,
      Configuration conf, String supportedScheme,
      boolean authorityRequired)
      throws IOException, URISyntaxException {
    super(theUri, new BaiduBosFileSystem(),
        conf, "bos", false);
  }

  /**
   * Returns the default port for the URI scheme.
   *
   * @return the default port
   */
  @Override
  public int getUriDefaultPort() {
    return super.getUriDefaultPort();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("bos:{");
    sb.append("URI =").append(fsImpl.getUri());
    sb.append("; fsImpl=").append(fsImpl);
    sb.append('}');
    return sb.toString();
  }

}
