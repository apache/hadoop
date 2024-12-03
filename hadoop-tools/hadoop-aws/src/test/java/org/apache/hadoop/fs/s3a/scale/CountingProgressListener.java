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

package org.apache.hadoop.fs.s3a.scale;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.impl.ProgressListener;
import org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.PUT_FAILED_EVENT;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.PUT_STARTED_EVENT;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.REQUEST_BYTE_TRANSFER_EVENT;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.TRANSFER_PART_FAILED_EVENT;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.TRANSFER_PART_STARTED_EVENT;

/**
 * Progress listener for AWS upload tracking.
 * Declared as {@link Progressable} to be passed down through the hadoop FS create()
 * operations, it also implements {@link ProgressListener} to get direct
 * information from the AWS SDK
 */
public class CountingProgressListener implements Progressable, ProgressListener {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSTestS3AHugeFiles.class);

  private final ContractTestUtils.NanoTimer timer;

  private final Map<ProgressListenerEvent, AtomicLong> eventCounts;

  private final AtomicLong bytesTransferred = new AtomicLong(0);

  /**
   * Create a progress listener.
   * @param timer timer to use
   */
  public CountingProgressListener(final ContractTestUtils.NanoTimer timer) {
    this.timer = timer;
    this.eventCounts = new EnumMap<>(ProgressListenerEvent.class);
    for (ProgressListenerEvent e : ProgressListenerEvent.values()) {
      this.eventCounts.put(e, new AtomicLong(0));
    }
  }

  /**
   * Create a progress listener with a nano timer.
   */
  public CountingProgressListener() {
    this(new ContractTestUtils.NanoTimer());
  }

  @Override
  public void progress() {
  }

  @Override
  public void progressChanged(ProgressListenerEvent eventType, long transferredBytes) {

    eventCounts.get(eventType).incrementAndGet();

    switch (eventType) {

    // part Upload has started
    case TRANSFER_PART_STARTED_EVENT:
    case PUT_STARTED_EVENT:
      LOG.info("Transfer started");
      break;

    // an upload part completed
    case TRANSFER_PART_COMPLETED_EVENT:
    case PUT_COMPLETED_EVENT:
      // completion
      bytesTransferred.addAndGet(transferredBytes);
      long elapsedTime = timer.elapsedTime();
      double elapsedTimeS = elapsedTime / 1.0e9;
      long written = bytesTransferred.get();
      long writtenMB = written / S3AScaleTestBase._1MB;
      LOG.info(String.format(
          "Event %s; total uploaded=%d MB in %.1fs;" + " effective upload bandwidth = %.2f MB/s",
          eventType, writtenMB, elapsedTimeS, writtenMB / elapsedTimeS));
      break;

    // and a transfer failed
    case PUT_FAILED_EVENT:
    case TRANSFER_PART_FAILED_EVENT:
      LOG.warn("Transfer failure");
      break;
    default:
      // nothing
      break;
    }
  }

  public String toString() {
    String sb =
        "ProgressCallback{" + "bytesTransferred=" + bytesTransferred.get() + '}';
    return sb;
  }

  /**
   * Get the count of a specific event.
   * @param key event
   * @return count
   */
  public long get(ProgressListenerEvent key) {
    return eventCounts.get(key).get();
  }

  /**
   * Get the number of bytes transferred.
   * @return byte count
   */
  public long getBytesTransferred() {
    return bytesTransferred.get();
  }

  /**
   * Get the number of event callbacks.
   * @return count of byte transferred events.
   */
  public long getUploadEvents() {
    return get(REQUEST_BYTE_TRANSFER_EVENT);
  }

  /**
   * Get count of started events.
   * @return count of started events.
   */
  public long getStartedEvents() {
    return get(PUT_STARTED_EVENT) + get(TRANSFER_PART_STARTED_EVENT);
  }

  /**
   * Get count of started events.
   * @return count of started events.
   */
  public long getFailures() {
    return get(PUT_FAILED_EVENT) + get(TRANSFER_PART_FAILED_EVENT);
  }

  /**
   * Verify that no failures took place.
   * @param operation operation being verified
   */
  public void verifyNoFailures(String operation) {
    Assertions.assertThat(getFailures())
        .describedAs("Failures in %s: %s", operation, this)
        .isEqualTo(0);
  }

  /**
   * Assert that the event count is as expected.
   * @param event event to look up
   * @return ongoing assertion
   */
  public AbstractLongAssert<?> assertEventCount(ProgressListenerEvent event) {
    return Assertions.assertThat(get(event)).describedAs("Event %s count", event);
  }

  /**
   * Assert that the event count is as expected.
   * @param event event to look up
   * @param count expected value.
   */
  public void assertEventCount(ProgressListenerEvent event, long count) {
    assertEventCount(event).isEqualTo(count);
  }
}
