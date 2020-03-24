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

package org.apache.hadoop.fs.s3a.impl.statistics;

import java.time.Duration;

/**
 * interface to receive statistics events from the AWS SDK
 * by way of {@link AwsStatisticsCollector}.
 */
public interface StatisticsFromAwsSdk {

  void updateAwsRequestCount(long count);

  void updateAwsRetryCount(long count);

  void updateAwsThrottleExceptionsCount(long count);

  void addAwsRequestTime(Duration duration);

  void addAwsClientExecuteTime(Duration duration);

  void addRequestMarshallTime(Duration duration);

  void addRequestSigningTime(Duration duration);

  void addResponseProcessingTime(Duration duration);
}

