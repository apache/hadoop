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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * S3A Implementation of the {@link BulkDelete} interface.
 */
public class BulkDeleteOperation extends AbstractStoreOperation implements BulkDelete {

  private final BulkDeleteOperationCallbacks callbacks;

  private final Path basePath;

  private final int pageSize;

  public BulkDeleteOperation(final StoreContext storeContext,
      final BulkDeleteOperationCallbacks callbacks,
      final Path basePath,
      final int pageSize,
      final AuditSpan span) {
    super(storeContext, span);
    this.callbacks = requireNonNull(callbacks);
    this.basePath = requireNonNull(basePath);
    checkArgument(pageSize > 0, "Page size must be greater than 0");
    this.pageSize = pageSize;
  }

  @Override
  public int pageSize() {
    return pageSize;
  }

  @Override
  public Path basePath() {
    return basePath;
  }

  @Override
  public BulkDeleteOutcome bulkDelete(final List<Path> paths)
      throws IOException, IllegalArgumentException {
    requireNonNull(paths);
    checkArgument(paths.size() <= pageSize,
        "Number of paths (%d) is larger than the page size (%d)", paths.size(), pageSize);

    final List<ObjectIdentifier> objects = paths.stream().map(p -> {
      checkArgument(p.isAbsolute(), "Path %s is not absolute", p);
      final String k = getStoreContext().pathToKey(p);
      return ObjectIdentifier.builder().key(k).build();
    }).collect(Collectors.toList());

    final List<String> errors = callbacks.bulkDelete(objects);
    if (!errors.isEmpty()) {

      final List<BulkDeleteOutcomeElement> outcomeElements = errors
          .stream()
          .map(error -> new BulkDeleteOutcomeElement(
              getStoreContext().keyToPath(error), error,
              null))
          .collect(Collectors.toList());
      return new BulkDeleteOutcome(outcomeElements);
    }
    return new BulkDeleteOutcome(Collections.emptyList());
  }

  @Override
  public void close() throws IOException {

  }

  public interface BulkDeleteOperationCallbacks {

    /**
     * Attempt a bulk delete operation.
     * @param keys key list
     * @return
     * @throws MultiObjectDeleteException one or more of the keys could not
     * be deleted in a multiple object delete operation.
     * @throws AwsServiceException amazon-layer failure.
     * @throws IOException other IO Exception.
     * @throws IllegalArgumentException illegal arguments
     */
    @Retries.RetryTranslated
    List<String> bulkDelete(final List<ObjectIdentifier> keys)
        throws MultiObjectDeleteException, IOException, IllegalArgumentException;
  }
}
