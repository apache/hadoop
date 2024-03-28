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

package org.apache.hadoop.fs.impl;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nonnull;

import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import static java.util.Objects.requireNonNull;

/**
 * Support for bulk delete operations.
 */
public final class BulkDeleteSupport {

  /**
   * Builder implementation; takes a callback for the actual operation.
   */
  public static class BulkDeleteBinding
      extends AbstractFSBuilderImpl<CompletableFuture<BulkDelete.Outcome>, BulkDelete.Builder>
      implements BulkDelete.Builder {

    private final RemoteIterator<Path> files;

    private final BulkDeleteBuilderCallbacks callbacks;

    private BulkDelete.DeleteProgress deleteProgress;

    public BulkDeleteBinding(
        @Nonnull final Path path,
        @Nonnull RemoteIterator<Path> files,
        @Nonnull BulkDeleteBuilderCallbacks callbacks) {
      super(path);
      this.files = requireNonNull(files);
      this.callbacks = requireNonNull(callbacks);
    }

    @Override
    public BulkDelete.Builder withProgress(final BulkDelete.DeleteProgress deleteProgress) {
      this.deleteProgress = deleteProgress;
      return this;
    }

    @Override
    public BulkDelete.Builder getThisBuilder() {
      return this;
    }

    public RemoteIterator<Path> getFiles() {
      return files;
    }

    public BulkDelete.DeleteProgress getDeleteProgress() {
      return deleteProgress;
    }

    @Override
    public CompletableFuture<BulkDelete.Outcome> build()
        throws IllegalArgumentException, IOException {
      return null;
    }
  }

  /**
   * Callbacks for the builder.
   */
  public interface BulkDeleteBuilderCallbacks {
    CompletableFuture<BulkDelete.Outcome> initiateBulkDelete(
        BulkDeleteBinding builder) throws IOException;
  }
}
