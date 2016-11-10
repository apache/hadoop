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

package org.apache.hadoop.fs.s3a;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.COPY_MAX_THREADS;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_COPY_MAX_THREADS;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_KEEPALIVE_TIME;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAX_THREADS;
import static org.apache.hadoop.fs.s3a.Constants.KEEPALIVE_TIME;
import static org.apache.hadoop.fs.s3a.Constants.MAX_THREADS;
import static org.apache.hadoop.fs.s3a.S3AUtils.getMaxThreads;
import static org.apache.hadoop.fs.s3a.S3AUtils.longOption;


/**
 * A wrapper around a {@link TransferManager} that lazily initializes the {@link TransferManager}.
 */
class LazyTransferManager {

  private final AmazonS3 s3;
  private final TransferManagerConfiguration transferConfiguration;
  private final int maxThreads;
  private final long keepAliveTime;
  private final String threadPoolName;

  private TransferManager transferManager;
  private ExecutorService executorService;

  private LazyTransferManager(AmazonS3 s3, TransferManagerConfiguration transferConfiguration, int maxThreads,
                      long keepAliveTime, String threadPoolName) {
    this.s3 = s3;
    this.transferConfiguration = transferConfiguration;
    this.maxThreads = maxThreads;
    this.keepAliveTime = keepAliveTime;
    this.threadPoolName = threadPoolName;
  }

  synchronized TransferManager get() {
    if (transferManager == null) {
      transferManager = createTransferManager();
      return transferManager;
    }
    return transferManager;
  }

  boolean isInitialized() {
    return transferManager == null;
  }

  ExecutorService getExecutorService() {
    return executorService;
  }

  private TransferManager createTransferManager() {
    executorService = new ThreadPoolExecutor(
            maxThreads, Integer.MAX_VALUE,
            keepAliveTime, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            BlockingThreadPoolExecutorService.newDaemonThreadFactory(
                    threadPoolName));

    TransferManager transferManager = new TransferManager(s3, executorService);
    transferManager.setConfiguration(transferConfiguration);
    return transferManager;
  }

  static LazyTransferManager createLazyUploadTransferManager(AmazonS3 s3, Configuration conf, long partSize,
                                                                     long multiPartThreshold) {
    int maxThreads = getMaxThreads(conf, MAX_THREADS, DEFAULT_MAX_THREADS);
    long keepAliveTime = longOption(conf, KEEPALIVE_TIME, DEFAULT_KEEPALIVE_TIME, 0);

    TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
    transferConfiguration.setMinimumUploadPartSize(partSize);
    transferConfiguration.setMultipartUploadThreshold(multiPartThreshold);

    return new LazyTransferManager(s3, transferConfiguration, maxThreads, keepAliveTime, "s3a-upload-unbounded");
  }

  static LazyTransferManager createLazyCopyTransferManager(AmazonS3 s3, Configuration conf, long partSize,
                                                                   long multiPartThreshold) {
    int maxThreads = getMaxThreads(conf, COPY_MAX_THREADS, DEFAULT_COPY_MAX_THREADS);
    long keepAliveTime = longOption(conf, KEEPALIVE_TIME, DEFAULT_KEEPALIVE_TIME, 0);

    TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
    transferConfiguration.setMultipartCopyPartSize(partSize);
    transferConfiguration.setMultipartCopyThreshold(multiPartThreshold);

    return new LazyTransferManager(s3, transferConfiguration, maxThreads, keepAliveTime, "s3a-copy-unbounded");
  }
}
