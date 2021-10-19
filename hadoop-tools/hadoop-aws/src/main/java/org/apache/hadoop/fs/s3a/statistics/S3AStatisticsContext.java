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

package org.apache.hadoop.fs.s3a.statistics;

import org.apache.hadoop.fs.s3a.s3guard.MetastoreInstrumentation;

/**
 * This is the statistics context for ongoing operations in S3A.
 */
public interface S3AStatisticsContext extends CountersAndGauges {

  /**
   * Get the metastore instrumentation.
   * @return an instance of the metastore statistics tracking.
   */
  MetastoreInstrumentation getS3GuardInstrumentation();

  /**
   * Create a stream input statistics instance.
   * @return the new instance
   */
  S3AInputStreamStatistics newInputStreamStatistics();

  /**
   * Create a new instance of the committer statistics.
   * @return a new committer statistics instance
   */
  CommitterStatistics newCommitterStatistics();

  /**
   * Create a stream output statistics instance.
   * @return the new instance
   */
  BlockOutputStreamStatistics newOutputStreamStatistics();

  /**
   * Create a delegation token statistics instance.
   * @return an instance of delegation token statistics
   */
  DelegationTokenStatistics newDelegationTokenStatistics();

  /**
   * Create a StatisticsFromAwsSdk instance.
   * @return an instance of StatisticsFromAwsSdk
   */
  StatisticsFromAwsSdk newStatisticsFromAwsSdk();

  /**
   * Creaet a multipart statistics collector.
   * @return an instance
   */
  S3AMultipartUploaderStatistics createMultipartUploaderStatistics();
}
