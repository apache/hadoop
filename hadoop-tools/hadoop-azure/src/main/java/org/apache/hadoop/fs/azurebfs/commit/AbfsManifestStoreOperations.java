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

package org.apache.hadoop.fs.azurebfs.commit;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.StoreOperationsThroughFileSystem;

/**
 * Extension of StoreOperationsThroughFileSystem with ABFS awareness.
 */
public class AbfsManifestStoreOperations extends StoreOperationsThroughFileSystem {

  public static final String NAME
      = "org.apache.hadoop.fs.azurebfs.commit.AbfsManifestStoreOperations";

  @Override
  public void bindToFileSystem(FileSystem filesystem, Path path) throws IOException {
    if (!(filesystem instanceof AzureBlobFileSystem)) {
      throw new PathIOException(path.toString(),
          "Not an abfs filesystem: " + filesystem.getClass());
    }
    super.bindToFileSystem(filesystem, path);
  }

  @Override
  public String getEtag(FileStatus status) {
    return AzureBlobFileSystemStore.extractVersion(status);
  }
}
