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

import java.util.ArrayList;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;

final class GcsListOperation {
  private static final int ALL = 0;
  private final Storage.BlobListOption[] listOptions;
  private final String bucketName;
  private final Storage storage;
  private final int limit;

  private GcsListOperation(Builder builder) {
    this.listOptions = builder.blobListOptions
        .toArray(new Storage.BlobListOption[builder.blobListOptions.size()]);
    this.bucketName = builder.bucket;
    this.storage = builder.storage;
    this.limit = builder.limit;
  }

  public List<Blob> execute() {
    List<Blob> result = new ArrayList<>();
    for (Blob blob : storage.list(bucketName, listOptions).iterateAll()) {
      result.add(blob);

      if (limit != ALL && result.size() >= limit) {
        break;
      }
    }

    return result;
  }

  static class Builder {
    private final ArrayList<Storage.BlobListOption> blobListOptions = new ArrayList<>();
    private String prefix;
    private final String bucket;
    private final Storage storage;
    private int limit = GcsListOperation.ALL;

    Builder(final String bucketName, final String thePrefix, Storage storage) {
      this.storage = storage;
      this.bucket = bucketName;
      this.prefix = thePrefix;
    }

    Builder forRecursiveListing() {
      return this;
    }

    GcsListOperation build() {
      // Can be null while listing the root directory.
      if (prefix != null) {
        blobListOptions.add(Storage.BlobListOption.prefix(prefix));
      }

      return new GcsListOperation(this);
    }

    Builder forCurrentDirectoryListing() {
      blobListOptions.add(Storage.BlobListOption.currentDirectory());
      blobListOptions.add(Storage.BlobListOption.includeTrailingDelimiter());
      return this;
    }

    Builder forImplicitDirectoryCheck() {
      this.limit = 1;
      if (prefix != null) {
        prefix = StringPaths.toDirectoryPath(prefix);
      }

      blobListOptions.add(Storage.BlobListOption.pageSize(1));
      forCurrentDirectoryListing();
      return this;
    }
  }
}
