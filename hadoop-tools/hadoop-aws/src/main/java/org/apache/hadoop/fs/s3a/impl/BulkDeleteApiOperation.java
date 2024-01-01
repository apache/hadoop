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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.amazonaws.AmazonClientException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.impl.BulkDeleteSupport;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.util.functional.FutureIO;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.emptyStatistics;
import static org.apache.hadoop.util.functional.RemoteIterators.mappingRemoteIterator;
import static org.apache.hadoop.util.functional.RemoteIterators.toList;

/**
 * Implementation of the public bulk delete API.
 * Iterates through the list of files, initiating bulk delete calls.
 */
public class BulkDeleteApiOperation
    extends ExecutingStoreOperation<CompletableFuture<BulkDelete.Outcome>> {


  private final BulkDeleteSupport.BulkDeleteBinding binding;


  /**
   * Number of entries in a page.
   */
  private final int pageSize;

  private final BulkDeleteApiOperationCallbacks callbacks;

  public BulkDeleteApiOperation(
      final StoreContext storeContext,
      final AuditSpan auditSpan,
      final BulkDeleteSupport.BulkDeleteBinding binding,
      final int pageSize,
      final BulkDeleteApiOperationCallbacks callbacks) {
    super(storeContext, auditSpan);
    this.binding = requireNonNull(binding);
    this.pageSize = pageSize;
    this.callbacks = requireNonNull(callbacks);
  }

  @Override
  public CompletableFuture<BulkDelete.Outcome> execute() throws IOException {
    // initial POC does a large blocking call.
    return FutureIO.eval(() -> {
      final StoreContext context = getStoreContext();
      final List<ObjectIdentifier> keysToDelete =
          toList(mappingRemoteIterator(binding.getFiles(),
              p -> ObjectIdentifier.builder().key(context.pathToKey(p)).build()));

      boolean successful = true;
      Exception exception = null;
      try {
        callbacks.removeKeys(keysToDelete);
      } catch (IOException e) {
        successful = false;
        exception = e;
      }
      return new BulkDelete.Outcome(
          successful,
          false,
          exception,
          keysToDelete.size(),
          0,
          1,
          emptyStatistics());
    });
  }

  public interface BulkDeleteApiOperationCallbacks {

    /**
     * Remove keys from the store.
     * @param keysToDelete collection of keys to delete on the s3-backend.
     * if empty, no request is made of the object store.
     * @throws InvalidRequestException if the request was rejected due to
     * a mistaken attempt to delete the root directory.
     * @throws MultiObjectDeleteException one or more of the keys could not
     * be deleted in a multiple object delete operation.
     * @throws IOException other IO Exception.
     */
    @Retries.RetryRaw
    void removeKeys(List<ObjectIdentifier> keysToDelete)
        throws MultiObjectDeleteException, IOException;
  }
}
