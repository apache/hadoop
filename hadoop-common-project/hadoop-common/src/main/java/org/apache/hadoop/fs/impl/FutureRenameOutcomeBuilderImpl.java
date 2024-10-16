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

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FutureRenameOutcomeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RenameOutcome;
import org.apache.hadoop.util.Progressable;

public class FutureRenameOutcomeBuilderImpl
    extends AbstractFSBuilderImpl<CompletableFuture<RenameOutcome>, FutureRenameOutcomeBuilder>
    implements FutureRenameOutcomeBuilder {


  @Nonnull private final Path dest;

  private boolean atomic;

  @Nullable private String etag;

  @Nullable private Progressable progressable;

  @Nullable private FileStatus status;

  public FutureRenameOutcomeBuilderImpl(
      @Nonnull final Path source, @Nonnull final Path dest) {
    super(source);
    this.dest = dest;
  }

  @Override
  public FutureRenameOutcomeBuilder withSourceStatus(@Nullable final FileStatus status) {
    this.status = status;
    return this;
  }

  @Override
  public FutureRenameOutcomeBuilder requireAtomic(final boolean atomic) {
    this.atomic = atomic;
    return this;
  }

  @Override
  public FutureRenameOutcomeBuilder withLegacyPathFixup(final boolean pathFixup) {
    return this;
  }

  @Override
  public FutureRenameOutcomeBuilder withProgress(@Nullable final Progressable progressable) {
    this.progressable = progressable;
    return this;
  }

  @Override
  public FutureRenameOutcomeBuilder withSourceEtag(@Nullable final String etag) {
    this.etag = etag;
    return this;
  }
}
