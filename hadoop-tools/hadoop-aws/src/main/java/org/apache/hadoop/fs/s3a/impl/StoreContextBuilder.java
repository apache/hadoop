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

import java.net.URI;

import com.google.common.util.concurrent.ListeningExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.s3guard.ITtlTimeProvider;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.security.UserGroupInformation;

public class StoreContextBuilder {

  private URI fsURI;

  private String bucket;

  private Configuration configuration;

  private String username;

  private UserGroupInformation owner;

  private ListeningExecutorService executor;

  private int executorCapacity;

  private Invoker invoker;

  private S3AInstrumentation instrumentation;

  private S3AStorageStatistics storageStatistics;

  private S3AInputPolicy inputPolicy = S3AInputPolicy.Normal;

  private ChangeDetectionPolicy changeDetectionPolicy;

  private boolean multiObjectDeleteEnabled = true;

  private MetadataStore metadataStore;

  private boolean useListV1 = false;

  private ContextAccessors contextAccessors;

  private ITtlTimeProvider timeProvider;

  public StoreContextBuilder setFsURI(final URI fsURI) {
    this.fsURI = fsURI;
    return this;
  }

  public StoreContextBuilder setBucket(final String bucket) {
    this.bucket = bucket;
    return this;
  }

  public StoreContextBuilder setConfiguration(final Configuration configuration) {
    this.configuration = configuration;
    return this;
  }

  public StoreContextBuilder setUsername(final String username) {
    this.username = username;
    return this;
  }

  public StoreContextBuilder setOwner(final UserGroupInformation owner) {
    this.owner = owner;
    return this;
  }

  public StoreContextBuilder setExecutor(final ListeningExecutorService executor) {
    this.executor = executor;
    return this;
  }

  public StoreContextBuilder setExecutorCapacity(final int executorCapacity) {
    this.executorCapacity = executorCapacity;
    return this;
  }

  public StoreContextBuilder setInvoker(final Invoker invoker) {
    this.invoker = invoker;
    return this;
  }

  public StoreContextBuilder setInstrumentation(final S3AInstrumentation instrumentation) {
    this.instrumentation = instrumentation;
    return this;
  }

  public StoreContextBuilder setStorageStatistics(final S3AStorageStatistics storageStatistics) {
    this.storageStatistics = storageStatistics;
    return this;
  }

  public StoreContextBuilder setInputPolicy(final S3AInputPolicy inputPolicy) {
    this.inputPolicy = inputPolicy;
    return this;
  }

  public StoreContextBuilder setChangeDetectionPolicy(final ChangeDetectionPolicy changeDetectionPolicy) {
    this.changeDetectionPolicy = changeDetectionPolicy;
    return this;
  }

  public StoreContextBuilder setMultiObjectDeleteEnabled(final boolean multiObjectDeleteEnabled) {
    this.multiObjectDeleteEnabled = multiObjectDeleteEnabled;
    return this;
  }

  public StoreContextBuilder setMetadataStore(final MetadataStore metadataStore) {
    this.metadataStore = metadataStore;
    return this;
  }

  public StoreContextBuilder setUseListV1(final boolean useListV1) {
    this.useListV1 = useListV1;
    return this;
  }

  public StoreContextBuilder setContextAccessors(final ContextAccessors contextAccessors) {
    this.contextAccessors = contextAccessors;
    return this;
  }

  public StoreContextBuilder setTimeProvider(final ITtlTimeProvider timeProvider) {
    this.timeProvider = timeProvider;
    return this;
  }

  @SuppressWarnings("deprecation")
  public StoreContext build() {
    return new StoreContext(fsURI, bucket, configuration, username, owner,
        executor, executorCapacity, invoker, instrumentation, storageStatistics,
        inputPolicy, changeDetectionPolicy, multiObjectDeleteEnabled,
        metadataStore, useListV1, contextAccessors, timeProvider);
  }
}
