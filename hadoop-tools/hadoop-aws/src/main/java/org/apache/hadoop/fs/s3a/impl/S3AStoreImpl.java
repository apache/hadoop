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
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.select.SelectBinding;
import org.apache.hadoop.util.DurationInfo;

/**
 * Implementation of S3A Store; depends on an RawS3A instance,
 * amongst other things.
 */
public class S3AStoreImpl extends AbstractS3AService
    implements S3AStore {

  public static final Logger LOG = LoggerFactory.getLogger(S3AStoreImpl.class);

  private RawS3A rawS3A;

  private Invoker s3guardInvoker;

  private ExecutorService unboundedThreadPool;

  private LocalDirAllocator directoryAllocator;

  private MetadataStore metadataStore;

  public S3AStoreImpl(final String name) {
    super(name);
  }

  public S3AStoreImpl() {
    this("s3astore");
  }

  @Override
  public void bind(
      final StoreContext storeContext,
      final RawS3A rawS3A,
      final Invoker s3guardInvoker,
      final ExecutorService unboundedThreadPool,
      final LocalDirAllocator directoryAllocator) {

    super.bind(storeContext);
    this.rawS3A = rawS3A;
    this.s3guardInvoker = s3guardInvoker;
    this.unboundedThreadPool = unboundedThreadPool;
    this.directoryAllocator = directoryAllocator;
    this.metadataStore = storeContext.getMetadataStore() ;
  }

  /**
   * Execute an S3 Select operation.
   * On a failure, the request is only logged at debug to avoid the
   * select exception being printed.
   * @param source source for selection
   * @param request Select request to issue.
   * @param action the action for use in exception creation
   * @return response
   * @throws IOException failure
   */
  @Override
  @Retries.RetryTranslated
  public SelectObjectContentResult select(
      final Path source,
      final SelectObjectContentRequest request,
      final String action)
      throws IOException {
    String bucketName = request.getBucketName();
    Preconditions.checkArgument(getBucket().equals(bucketName),
        "wrong bucket: %s", bucketName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initiating select call {} {}",
          source, request.getExpression());
      LOG.debug(SelectBinding.toString(request));
    }
    return getInvoker().retry(
        action,
        source.toString(),
        true,
        () -> {
          try (DurationInfo ignored =
                   new DurationInfo(LOG, "S3 Select operation")) {
            try {
              return rawS3A.select(request);
            } catch (AmazonS3Exception e) {
              LOG.error("Failure of S3 Select request against {}",
                  source);
              LOG.debug("S3 Select request against {}:\n{}",
                  source,
                  SelectBinding.toString(request),
                  e);
              throw e;
            }
          }
        });
  }


}
