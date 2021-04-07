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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.s3a.s3guard.ITtlTimeProvider;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.LambdaUtils;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;

/**
 * This class provides the core context of the S3A filesystem to subsidiary
 * components, without exposing the entire parent class.
 * This is eliminate explicit recursive coupling.
 *
 * Where methods on the FS are to be invoked, they are referenced
 * via the {@link ContextAccessors} interface, so tests can implement
 * their own.
 *
 * <i>Warning:</i> this really is private and unstable. Do not use
 * outside the org.apache.hadoop.fs.s3a package, or in extension points
 * such as DelegationTokens.
 */
@InterfaceAudience.LimitedPrivate("S3A Filesystem and extensions")
@InterfaceStability.Unstable
public class StoreContext {

  /** Filesystem URI. */
  private final URI fsURI;

  /** Bucket name. */
  private final String bucket;

  /** FS configuration after all per-bucket overrides applied. */
  private final Configuration configuration;

  /** Username. */
  private final String username;

  /** Principal who created the FS. */
  private final UserGroupInformation owner;

  /**
   * Bounded thread pool for async operations.
   */
  private final ListeningExecutorService executor;

  /**
   * Capacity of new executors created.
   */
  private final int executorCapacity;

  /** Invoker of operations. */
  private final Invoker invoker;

  /** Instrumentation and statistics. */
  private final S3AStatisticsContext instrumentation;

  private final S3AStorageStatistics storageStatistics;

  /** Seek policy. */
  private final S3AInputPolicy inputPolicy;

  /** How to react to changes in etags and versions. */
  private final ChangeDetectionPolicy changeDetectionPolicy;

  /** Evaluated options. */
  private final boolean multiObjectDeleteEnabled;

  /** List algorithm. */
  private final boolean useListV1;

  /**
   * To allow this context to be passed down to the metastore, this field
   * wll be null until initialized.
   */
  private final MetadataStore metadataStore;

  private final ContextAccessors contextAccessors;

  /**
   * Source of time.
   */
  private ITtlTimeProvider timeProvider;

  /**
   * Instantiate.
   * @deprecated as public method: use {@link StoreContextBuilder}.
   */
  public StoreContext(
      final URI fsURI,
      final String bucket,
      final Configuration configuration,
      final String username,
      final UserGroupInformation owner,
      final ExecutorService executor,
      final int executorCapacity,
      final Invoker invoker,
      final S3AStatisticsContext instrumentation,
      final S3AStorageStatistics storageStatistics,
      final S3AInputPolicy inputPolicy,
      final ChangeDetectionPolicy changeDetectionPolicy,
      final boolean multiObjectDeleteEnabled,
      final MetadataStore metadataStore,
      final boolean useListV1,
      final ContextAccessors contextAccessors,
      final ITtlTimeProvider timeProvider) {
    this.fsURI = fsURI;
    this.bucket = bucket;
    this.configuration = configuration;
    this.username = username;
    this.owner = owner;
    this.executor = MoreExecutors.listeningDecorator(executor);
    this.executorCapacity = executorCapacity;
    this.invoker = invoker;
    this.instrumentation = instrumentation;
    this.storageStatistics = storageStatistics;
    this.inputPolicy = inputPolicy;
    this.changeDetectionPolicy = changeDetectionPolicy;
    this.multiObjectDeleteEnabled = multiObjectDeleteEnabled;
    this.metadataStore = metadataStore;
    this.useListV1 = useListV1;
    this.contextAccessors = contextAccessors;
    this.timeProvider = timeProvider;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public URI getFsURI() {
    return fsURI;
  }

  public String getBucket() {
    return bucket;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public String getUsername() {
    return username;
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  public Invoker getInvoker() {
    return invoker;
  }

  /**
   * Get the statistics context for this StoreContext.
   * @return the statistics context this store context was created
   * with.
   */
  public S3AStatisticsContext getInstrumentation() {
    return instrumentation;
  }

  public S3AInputPolicy getInputPolicy() {
    return inputPolicy;
  }

  public ChangeDetectionPolicy getChangeDetectionPolicy() {
    return changeDetectionPolicy;
  }

  public boolean isMultiObjectDeleteEnabled() {
    return multiObjectDeleteEnabled;
  }

  public MetadataStore getMetadataStore() {
    return metadataStore;
  }

  public boolean isUseListV1() {
    return useListV1;
  }

  public ContextAccessors getContextAccessors() {
    return contextAccessors;
  }

  /**
   * Convert a key to a fully qualified path.
   * @param key input key
   * @return the fully qualified path including URI scheme and bucket name.
   */
  public Path keyToPath(String key) {
    return contextAccessors.keyToPath(key);
  }

  /**
   * Turns a path (relative or otherwise) into an S3 key.
   *
   * @param path input path, may be relative to the working dir
   * @return a key excluding the leading "/", or, if it is the root path, ""
   */
  public String pathToKey(Path path) {
    return contextAccessors.pathToKey(path);
  }

  /**
   * Qualify a path.
   *
   * @param path path to qualify/normalize
   * @return possibly new path.
   */
  public Path makeQualified(Path path) {
    return contextAccessors.makeQualified(path);
  }

  /**
   * Get the storage statistics of this filesystem.
   * @return the storage statistics
   */
  public S3AStorageStatistics getStorageStatistics() {
    return storageStatistics;
  }

  /**
   * Increment a statistic by 1.
   * This increments both the instrumentation and storage statistics.
   * @param statistic The operation to increment
   */
  public void incrementStatistic(Statistic statistic) {
    incrementStatistic(statistic, 1);
  }

  /**
   * Increment a statistic by a specific value.
   * This increments both the instrumentation and storage statistics.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  public void incrementStatistic(Statistic statistic, long count) {
    instrumentation.incrementCounter(statistic, count);
  }

  /**
   * Decrement a gauge by a specific value.
   * @param statistic The operation to decrement
   * @param count the count to decrement
   */
  public void decrementGauge(Statistic statistic, long count) {
    instrumentation.decrementGauge(statistic, count);
  }

  /**
   * Increment a gauge by a specific value.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  public void incrementGauge(Statistic statistic, long count) {
    instrumentation.incrementGauge(statistic, count);
  }

  /**
   * Create a new executor service with a given capacity.
   * This executor submits works to the {@link #executor}, using a
   * {@link SemaphoredDelegatingExecutor} to limit the number
   * of requests coming in from a specific client.
   *
   * Because this delegates to an existing thread pool, the cost of
   * creating a new instance here is low.
   * As the throttling is per instance, separate instances
   * should be created for each operation which wishes to execute work in
   * parallel <i>without</i> saturating the base executor.
   * This is important if either the duration of each operation is long
   * or the submission rate of work is high.
   * @param capacity maximum capacity of this executor.
   * @return an executor for submitting work.
   */
  public ExecutorService createThrottledExecutor(int capacity) {
    return new SemaphoredDelegatingExecutor(executor,
        capacity, true);
  }

  /**
   * Create a new executor with the capacity defined in
   * {@link #executorCapacity}.
   * @return a new executor for exclusive use by the caller.
   */
  public ExecutorService createThrottledExecutor() {
    return createThrottledExecutor(executorCapacity);
  }

  /**
   * Get the owner of the filesystem.
   * @return the user who created this filesystem.
   */
  public UserGroupInformation getOwner() {
    return owner;
  }

  /**
   * Create a temporary file somewhere.
   * @param prefix prefix for the temporary file
   * @param size expected size.
   * @return a file reference.
   * @throws IOException failure.
   */
  public File createTempFile(String prefix, long size) throws IOException {
    return contextAccessors.createTempFile(prefix, size);
  }

  /**
   * Get the location of the bucket.
   * @return the bucket location.
   * @throws IOException failure.
   */
  public String getBucketLocation() throws IOException {
    return contextAccessors.getBucketLocation();
  }

  /**
   * Get the time provider.
   * @return the time source.
   */
  public ITtlTimeProvider getTimeProvider() {
    return timeProvider;
  }

  /**
   * Build the full S3 key for a request from the status entry,
   * possibly adding a "/" if it represents directory and it does
   * not have a trailing slash already.
   * @param stat status to build the key from
   * @return a key for a delete request
   */
  public String fullKey(final S3AFileStatus stat) {
    String k = pathToKey(stat.getPath());
    return (stat.isDirectory() && !k.endsWith("/"))
        ? k + "/"
        : k;
  }

  /**
   * Submit a closure for execution in the executor
   * returned by {@link #getExecutor()}.
   * @param <T> type of future
   * @param future future for the result.
   * @param call callable to invoke.
   * @return the future passed in
   */
  public <T> CompletableFuture<T> submit(
      final CompletableFuture<T> future,
      final Callable<T> call) {
    getExecutor().submit(() ->
        LambdaUtils.eval(future, call));
    return future;
  }
}
