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

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;

/**
 * Implementation of S3A Store; depends on an RawS3A instance,
 * amongst other things.
 */
public class S3AStoreImpl extends AbstractS3AService
    implements S3AStore {

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
}
