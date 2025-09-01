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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.HttpChannelEOFException;
import org.apache.hadoop.fs.s3a.S3ARetryPolicy;
import org.apache.hadoop.net.ConnectTimeoutException;
import software.amazon.s3.analyticsaccelerator.util.retry.DefaultRetryStrategyImpl;
import software.amazon.s3.analyticsaccelerator.util.retry.RetryPolicy;
import software.amazon.s3.analyticsaccelerator.util.retry.RetryStrategy;

import java.io.EOFException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;

import static org.apache.hadoop.fs.s3a.Constants.RETRY_LIMIT;
import static org.apache.hadoop.fs.s3a.Constants.RETRY_LIMIT_DEFAULT;

public class AnalyticsStreamRetryPolicy extends S3ARetryPolicy {

  private final RetryStrategy strategy;

  /**
   * Instantiate.
   *
   * @param conf configuration to read.
   */
  public AnalyticsStreamRetryPolicy(Configuration conf) {
    super(conf);
    int limit = conf.getInt(RETRY_LIMIT, RETRY_LIMIT_DEFAULT);

    RetryPolicy connectivityFailure = connectivityFailure(limit);
    this.strategy = new DefaultRetryStrategyImpl(connectivityFailure);
  }

  public RetryStrategy getAnalyticsRetryStrategy() {
    return this.strategy;
  }

  private RetryPolicy connectivityFailure(int limit) {
    List<Class<? extends Throwable>> retryableExceptions = ImmutableList.of(
        HttpChannelEOFException.class,
        ConnectTimeoutException.class,
        ConnectException.class,
        EOFException.class,
        SocketException.class,
        SocketTimeoutException.class
    );

    return RetryPolicy.builder().handle(retryableExceptions).withMaxRetries(limit).build();
  }

}
