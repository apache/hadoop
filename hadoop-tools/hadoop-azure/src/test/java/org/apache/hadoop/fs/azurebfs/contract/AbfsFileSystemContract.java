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

package org.apache.hadoop.fs.azurebfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

/**
 * Azure BlobFileSystem Contract. Test paths are created using any maven fork
 * identifier, if defined. This guarantees paths unique to tests
 * running in parallel.
 */
public class AbfsFileSystemContract extends AbstractBondedFSContract {

  public static final String CONTRACT_XML = "abfs.xml";
  private final boolean isSecure;

  protected AbfsFileSystemContract(final Configuration conf, boolean secure) {
    super(conf);
    //insert the base features
    addConfResource(CONTRACT_XML);
    this.isSecure = secure;
  }

  @Override
  public String getScheme() {
    return isSecure ? FileSystemUriSchemes.ABFS_SECURE_SCHEME
            : FileSystemUriSchemes.ABFS_SCHEME;
  }

  @Override
  public Path getTestPath() {
    return new Path(UriUtils.generateUniqueTestPath());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbfsFileSystemContract{");
    sb.append("isSecure=").append(isSecure);
    sb.append(super.toString());
    sb.append('}');
    return sb.toString();
  }
}
