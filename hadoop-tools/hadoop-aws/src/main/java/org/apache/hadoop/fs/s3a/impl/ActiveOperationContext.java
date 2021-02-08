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

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;

/**
 * Context for any active operation.
 */
public class ActiveOperationContext {

  /**
   * An operation ID; currently just for logging...proper tracing needs more.
   */
  private final long operationId;

  /**
   * Statistics context.
   */
  private final S3AStatisticsContext statisticsContext;

  /**
   * S3Guard bulk operation state, if (currently) set.
   */
  @Nullable private BulkOperationState bulkOperationState;

  public ActiveOperationContext(
      final long operationId,
      final S3AStatisticsContext statisticsContext,
      @Nullable final BulkOperationState bulkOperationState) {
    this.operationId = operationId;
    this.statisticsContext = Objects.requireNonNull(statisticsContext,
        "null statistics context");
    this.bulkOperationState = bulkOperationState;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "ActiveOperation{");
    sb.append("operationId=").append(operationId);
    sb.append(", bulkOperationState=").append(bulkOperationState);
    sb.append('}');
    return sb.toString();
  }

  @Nullable
  public BulkOperationState getBulkOperationState() {
    return bulkOperationState;
  }

  public long getOperationId() {
    return operationId;
  }

  public S3AStatisticsContext getS3AStatisticsContext() {
    return statisticsContext;
  }

  private static final AtomicLong NEXT_OPERATION_ID = new AtomicLong(0);

  /**
   * Create an operation ID. The nature of it should be opaque.
   * @return an ID for the constructor.
   */
  protected static long newOperationId() {
    return NEXT_OPERATION_ID.incrementAndGet();
  }

}
