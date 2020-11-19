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

package org.apache.hadoop.fs.s3a.test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.ITtlTimeProvider;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;
import org.apache.hadoop.fs.s3a.s3guard.RenameTracker;

/**
 * MetadataStore which tracks what is deleted and added.
 */
public class OperationTrackingStore implements MetadataStore {

  private final List<Path> deleted = new ArrayList<>();

  private final List<Path> created = new ArrayList<>();

  @Override
  public void initialize(final FileSystem fs,
      ITtlTimeProvider ttlTimeProvider) {
  }

  @Override
  public void initialize(final Configuration conf,
      ITtlTimeProvider ttlTimeProvider) {
  }

  @Override
  public void forgetMetadata(final Path path) {
  }

  @Override
  public PathMetadata get(final Path path) {
    return null;
  }

  @Override
  public PathMetadata get(final Path path,
      final boolean wantEmptyDirectoryFlag) {
    return null;
  }

  @Override
  public DirListingMetadata listChildren(final Path path) {
    return null;
  }

  @Override
  public void put(final PathMetadata meta) {
    put(meta, null);
  }

  @Override
  public void put(final PathMetadata meta,
      final BulkOperationState operationState) {
    created.add(meta.getFileStatus().getPath());
  }

  @Override
  public void put(final Collection<? extends PathMetadata> metas,
      final BulkOperationState operationState) {
    metas.stream().forEach(meta -> put(meta, null));
  }

  @Override
  public void put(final DirListingMetadata meta,
      final List<Path> unchangedEntries,
      final BulkOperationState operationState) {
    created.add(meta.getPath());
  }

  @Override
  public void destroy() {
  }

  @Override
  public void delete(final Path path,
      final BulkOperationState operationState) {
    deleted.add(path);
  }

  @Override
  public void deletePaths(final Collection<Path> paths,
      @Nullable final BulkOperationState operationState)
      throws IOException {
    deleted.addAll(paths);
  }

  @Override
  public void deleteSubtree(final Path path,
      final BulkOperationState operationState) {

  }

  @Override
  public void move(@Nullable final Collection<Path> pathsToDelete,
      @Nullable final Collection<PathMetadata> pathsToCreate,
      @Nullable final BulkOperationState operationState) {
  }

  @Override
  public void prune(final PruneMode pruneMode, final long cutoff) {
  }

  @Override
  public long prune(final PruneMode pruneMode,
      final long cutoff,
      final String keyPrefix) {
    return 0;
  }

  @Override
  public BulkOperationState initiateBulkWrite(
      final BulkOperationState.OperationType operation,
      final Path dest) {
    return null;
  }

  @Override
  public void setTtlTimeProvider(ITtlTimeProvider ttlTimeProvider) {
  }

  @Override
  public Map<String, String> getDiagnostics() {
    return null;
  }

  @Override
  public void updateParameters(final Map<String, String> parameters) {
  }

  @Override
  public void close() {
  }

  public List<Path> getDeleted() {
    return deleted;
  }

  public List<Path> getCreated() {
    return created;
  }

  @Override
  public RenameTracker initiateRenameOperation(
      final StoreContext storeContext,
      final Path source,
      final S3AFileStatus sourceStatus,
      final Path dest) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public void addAncestors(final Path qualifiedPath,
      @Nullable final BulkOperationState operationState) {

  }
}
