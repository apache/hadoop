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
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.s3a.s3guard.ITtlTimeProvider;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Builder for the store context.
 */
public class StoreContextBuilder {

  private URI fsURI;

  private String bucket;

  private Configuration configuration;

  private String username;

  private UserGroupInformation owner;

  private ExecutorService executor;

  private int executorCapacity;

  private Invoker invoker;

  private S3AStatisticsContext instrumentation;

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

  public StoreContextBuilder setBucket(final String b) {
    this.bucket = b;
    return this;
  }

  public StoreContextBuilder setConfiguration(final Configuration conf) {
    this.configuration = conf;
    return this;
  }

  public StoreContextBuilder setUsername(final String user) {
    this.username = user;
    return this;
  }

  public StoreContextBuilder setOwner(final UserGroupInformation ugi) {
    this.owner = ugi;
    return this;
  }

  public StoreContextBuilder setExecutor(
      final ExecutorService ex) {
    this.executor = ex;
    return this;
  }

  public StoreContextBuilder setExecutorCapacity(
      final int capacity) {
    this.executorCapacity = capacity;
    return this;
  }

  public StoreContextBuilder setInvoker(final Invoker invoke) {
    this.invoker = invoke;
    return this;
  }

  public StoreContextBuilder setInstrumentation(
      final S3AStatisticsContext instr) {
    this.instrumentation = instr;
    return this;
  }

  public StoreContextBuilder setStorageStatistics(
      final S3AStorageStatistics sstats) {
    this.storageStatistics = sstats;
    return this;
  }

  public StoreContextBuilder setInputPolicy(
      final S3AInputPolicy policy) {
    this.inputPolicy = policy;
    return this;
  }

  public StoreContextBuilder setChangeDetectionPolicy(
      final ChangeDetectionPolicy policy) {
    this.changeDetectionPolicy = policy;
    return this;
  }

  public StoreContextBuilder setMultiObjectDeleteEnabled(
      final boolean enabled) {
    this.multiObjectDeleteEnabled = enabled;
    return this;
  }

  public StoreContextBuilder setMetadataStore(
      final MetadataStore store) {
    this.metadataStore = store;
    return this;
  }

  public StoreContextBuilder setUseListV1(
      final boolean useV1) {
    this.useListV1 = useV1;
    return this;
  }

  public StoreContextBuilder setContextAccessors(
      final ContextAccessors accessors) {
    this.contextAccessors = accessors;
    return this;
  }

  public StoreContextBuilder setTimeProvider(
      final ITtlTimeProvider provider) {
    this.timeProvider = provider;
    return this;
  }

  @SuppressWarnings("deprecation")
  public StoreContext build() {
    return new StoreContext(fsURI,
        bucket,
        configuration,
        username,
        owner,
        executor,
        executorCapacity,
        invoker,
        instrumentation,
        storageStatistics,
        inputPolicy,
        changeDetectionPolicy,
        multiObjectDeleteEnabled,
        metadataStore,
        useListV1,
        contextAccessors,
        timeProvider);
  }
}
