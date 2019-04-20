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
import java.util.Optional;
import java.util.function.Function;

import com.google.common.util.concurrent.ListeningExecutorService;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class provides the core context of the S3A filesystem to subsidiary
 * components, without exposing the entire parent class.
 * This is eliminate explicit recursive coupling.
 *
 * <i>Warning:</i> this really is private and unstable. Do not use
 * outside the org.apache.hadoop.fs.s3a package.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StoreContext {

  /*
   * Foundational fields.
   */
  /** Filesystem URI. */
  private final URI fsURI;

  /** Bucket name */
  private final String bucket;

  /* FS configuration after all per-bucket overrides applied. */
  private final Configuration configuration;

  /** Username. */
  private final String username;

  /** Principal who created the FS*/
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
  private final ListeningExecutorService boundedThreadPool;

  /** Invoker of operations. */
  private final Invoker invoker;

  private final LocalDirAllocator directoryAllocator;

  /* Instrumentation and statistics. */
  private final S3AInstrumentation instrumentation;
  private final S3AStorageStatistics storageStatistics;

  /** Seek policy. */
  private final S3AInputPolicy inputPolicy;

  /** How to react to changes in etags and versions. */
  private final ChangeDetectionPolicy changeDetectionPolicy;

  /** Evaluated options. */
  private final boolean enableMultiObjectsDelete;

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

  public StoreContext(final URI fsURI,
      final String bucket,
      final Configuration configuration,
      final String username,
      final UserGroupInformation owner,
      final ListeningExecutorService boundedThreadPool,
      final Invoker invoker,
      final LocalDirAllocator directoryAllocator,
      final S3AInstrumentation instrumentation,
      final S3AStorageStatistics storageStatistics,
      final S3AInputPolicy inputPolicy,
      final ChangeDetectionPolicy changeDetectionPolicy,
      final boolean enableMultiObjectsDelete,
      final MetadataStore metadataStore,
      final Function<String, Path> keyToPathQualifier,
      final String bucketLocation,
      final boolean useListV1,
      final boolean versioned) {
    this.fsURI = fsURI;
    this.bucket = bucket;
    this.configuration = configuration;
    this.username = username;
    this.owner = owner;
    this.boundedThreadPool = boundedThreadPool;
    this.invoker = invoker;
    this.directoryAllocator = directoryAllocator;
    this.instrumentation = instrumentation;
    this.storageStatistics = storageStatistics;
    this.inputPolicy = inputPolicy;
    this.changeDetectionPolicy = changeDetectionPolicy;
    this.enableMultiObjectsDelete = enableMultiObjectsDelete;
    this.metadataStore = metadataStore;
    this.keyToPathQualifier = keyToPathQualifier;
    this.bucketLocation = Optional.ofNullable(bucketLocation);
    this.useListV1 = useListV1;
    this.versioned = versioned;
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

  public ListeningExecutorService getBoundedThreadPool() {
    return boundedThreadPool;
  }

  public Invoker getInvoker() {
    return invoker;
  }

  public LocalDirAllocator getDirectoryAllocator() {
    return directoryAllocator;
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

  public boolean isEnableMultiObjectsDelete() {
    return enableMultiObjectsDelete;
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


}
