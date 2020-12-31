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

package org.apache.hadoop.fs.s3a.statistics.impl;


import java.io.IOException;
import java.util.Objects;
import java.util.function.BiConsumer;

import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.statistics.S3AMultipartUploaderStatistics;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_ABORTED;
import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_ABORT_UNDER_PATH_INVOKED;
import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_COMPLETED;
import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_INSTANTIATED;
import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_PART_PUT;
import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_PART_PUT_BYTES;
import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_STARTED;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/**
 * Implementation of the uploader statistics.
 * <p>
 * This takes a function to update some counter and will update
 * this value when things change, so it can be bonded to arbitrary
 * statistic collectors.
 * </p>
 * <p>
 * Internally it builds a map of the relevant multipart statistics,
 * increments as appropriate and serves this data back through
 * the {@code IOStatisticsSource} API.
 * </p>
 */
public final class S3AMultipartUploaderStatisticsImpl
    extends AbstractS3AStatisticsSource
    implements S3AMultipartUploaderStatistics {

  /**
   * The operation to increment a counter/statistic by a value.
   */
  private final BiConsumer<Statistic, Long> incrementCallback;

  /**
   * Constructor.
   * @param incrementCallback  The operation to increment a
   * counter/statistic by a value.
   */
  public S3AMultipartUploaderStatisticsImpl(
      final BiConsumer<Statistic, Long> incrementCallback) {
    this.incrementCallback = Objects.requireNonNull(incrementCallback);
    IOStatisticsStore st = iostatisticsStore()
        .withCounters(
            MULTIPART_UPLOAD_INSTANTIATED.getSymbol(),
            MULTIPART_UPLOAD_PART_PUT.getSymbol(),
            MULTIPART_UPLOAD_PART_PUT_BYTES.getSymbol(),
            MULTIPART_UPLOAD_ABORTED.getSymbol(),
            MULTIPART_UPLOAD_ABORT_UNDER_PATH_INVOKED.getSymbol(),
            MULTIPART_UPLOAD_COMPLETED.getSymbol(),
            MULTIPART_UPLOAD_STARTED.getSymbol())
        .build();
    setIOStatistics(st);
  }

  private void inc(Statistic op, long count) {
    incrementCallback.accept(op, count);
    incCounter(op.getSymbol(), count);
  }

  @Override
  public void instantiated() {
    inc(MULTIPART_UPLOAD_INSTANTIATED, 1);
  }

  @Override
  public void uploadStarted() {
    inc(MULTIPART_UPLOAD_STARTED, 1);
  }

  @Override
  public void partPut(final long lengthInBytes) {
    inc(MULTIPART_UPLOAD_PART_PUT, 1);
    inc(MULTIPART_UPLOAD_PART_PUT_BYTES, lengthInBytes);
  }

  @Override
  public void uploadCompleted() {
    inc(MULTIPART_UPLOAD_COMPLETED, 1);
  }

  @Override
  public void uploadAborted() {
    inc(MULTIPART_UPLOAD_ABORTED, 1);
  }

  @Override
  public void abortUploadsUnderPathInvoked() {
    inc(MULTIPART_UPLOAD_ABORT_UNDER_PATH_INVOKED, 1);
  }

  @Override
  public void close() throws IOException {

  }
}
