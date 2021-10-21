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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.EtagExtractor;

/**
 * Extension of StoreOperationsThroughFileSystem which can extract the
 * etag of files.
 */
public class AbfsEtagExtractor implements EtagExtractor {

  @Override
  public String getEtag(FileStatus status) {
    return AzureBlobFileSystemStore.extractVersion(status);
  }
}