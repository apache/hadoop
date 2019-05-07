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
import java.util.Optional;
import java.util.function.Function;

import com.google.common.util.concurrent.ListeningExecutorService;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;

/**
 * This class provides the core context of the S3A filesystem to subsidiary
 * components, without exposing the entire parent class.
 * This is eliminate explicit recursive coupling.
 *
 * Where methods on the FS are to be invoked, they are all passed in
 * via functional interfaces, so test setups can pass in mock callbacks
 * instead.
 *
 * <i>Warning:</i> this really is private and unstable. Do not use
 * outside the org.apache.hadoop.fs.s3a package.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StoreContext {

  /** Filesystem URI. */
  private final URI fsURI;

  /** Bucket name */
  private final String bucket;

  /** FS configuration after all per-bucket overrides applied. */
  private final Configuration configuration;

  /** Username. */
  private final String username;

  /** Principal who created the FS. */
  private final UserGroupInformation owner;

  /**
   * Location of a bucket.
   * Optional as the AWS call to evaluate this may fail from a permissions
   * or other IOE.
   */
  public final Optional<String> bucketLocation;

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

  /* Instrumentation and statistics. */
  private final S3AInstrumentation instrumentation;
  private final S3AStorageStatistics storageStatistics;

  /** Seek policy. */
  private final S3AInputPolicy inputPolicy;

  /** How to react to changes in etags and versions. */
  private final ChangeDetectionPolicy changeDetectionPolicy;

  /** Evaluated options. */
  private final boolean multiObjectDeleteEnabled;

  /** List algorithm. */
  private final boolean useListV1;

  /** Is the store versioned? */
  private final boolean versioned;

  /**
   * To allow this context to be passed down to the metastore, this field
   * wll be null until initialized.
   */
  private final MetadataStore metadataStore;

  /** Function to take a key and return a path. */
  private final Function<String, Path> keyToPathQualifier;

  /** Factory for temporary files. */
  private final TempFileFactory tempFileFactory;

  /**
   * Instantiate.
   * No attempt to use a builder here as outside tests
   * this should only be created in the S3AFileSystem.
   */
  public StoreContext(final URI fsURI,
      final String bucket,
      final Configuration configuration,
      final String username,
      final UserGroupInformation owner,
      final ListeningExecutorService executor,
      final int executorCapacity,
      final Invoker invoker,
      final S3AInstrumentation instrumentation,
      final S3AStorageStatistics storageStatistics,
      final S3AInputPolicy inputPolicy,
      final ChangeDetectionPolicy changeDetectionPolicy,
      final boolean multiObjectDeleteEnabled,
      final MetadataStore metadataStore,
      final Function<String, Path> keyToPathQualifier,
      final String bucketLocation,
      final boolean useListV1,
      final boolean versioned,
      final TempFileFactory tempFileFactory) {
    this.fsURI = fsURI;
    this.bucket = bucket;
    this.configuration = configuration;
    this.username = username;
    this.owner = owner;
    this.executor = executor;
    this.executorCapacity = executorCapacity;
    this.invoker = invoker;
    this.instrumentation = instrumentation;
    this.storageStatistics = storageStatistics;
    this.inputPolicy = inputPolicy;
    this.changeDetectionPolicy = changeDetectionPolicy;
    this.multiObjectDeleteEnabled = multiObjectDeleteEnabled;
    this.metadataStore = metadataStore;
    this.keyToPathQualifier = keyToPathQualifier;
    this.bucketLocation = Optional.ofNullable(bucketLocation);
    this.useListV1 = useListV1;
    this.versioned = versioned;
    this.tempFileFactory = tempFileFactory;
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

  public Optional<String> getBucketLocation() {
    return bucketLocation;
  }

  public ListeningExecutorService getExecutor() {
    return executor;
  }

  public Invoker getInvoker() {
    return invoker;
  }

  public S3AInstrumentation getInstrumentation() {
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

  public boolean isVersioned() {
    return versioned;
  }

  public Function<String, Path> getKeyToPathQualifier() {
    return keyToPathQualifier;
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
    storageStatistics.incrementCounter(statistic, count);
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
  public ListeningExecutorService createThrottledExecutor(int capacity) {
    return new SemaphoredDelegatingExecutor(executor,
        capacity, true);
  }

  /**
   * Create a new executor with the capacity defined in
   * {@link #executorCapacity}
   * @return a new executor for exclusive use by the caller.
   */
  public ListeningExecutorService createThrottledExecutor() {
    return createThrottledExecutor(executorCapacity);
  }

  public UserGroupInformation getOwner() {
    return owner;
  }

  public File createTempFile(String pathStr, long size) throws IOException {
    return tempFileFactory.createTempFile(pathStr, size);
  }

  /**
   * The interface for temporary files.
   * The standard Java 8 BiFunction cannot be used as it doesn't raise an
   * IOE.
   */
  @FunctionalInterface
  public interface TempFileFactory {
    File createTempFile(String pathStr, long size) throws IOException;
  }
}
