/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class ReadBufferManagerV2 implements ReadBufferManager {

  /**
   *
   * @param stream
   * @param requestedOffset
   * @param requestedLength
   * @param tracingContext
   */
  @Override
  public void queueReadAhead(final AbfsInputStream stream,
      final long requestedOffset,
      final int requestedLength,
      final TracingContext tracingContext) {

  }

  /**
   *
   * @param stream
   * @param position
   * @param length
   * @param buffer
   * @return
   * @throws IOException
   */
  @Override
  public int getBlock(final AbfsInputStream stream,
      final long position,
      final int length,
      final byte[] buffer) throws IOException {
    return 0;
  }

  /**
   *
   * @return
   * @throws InterruptedException
   */
  @Override
  public ReadBuffer getNextBlockToRead() throws InterruptedException {
    return null;
  }

  /**
   *
   * @param buffer
   * @param result
   * @param bytesActuallyRead
   */
  @Override
  public void doneReading(final ReadBuffer buffer,
      final ReadBufferStatus result,
      final int bytesActuallyRead) {

  }

  /**
   *
   * @param stream
   */
  @Override
  public void purgeBuffersForStream(final AbfsInputStream stream) {

  }

  /**
   *
   */
  @Override
  public void testResetReadBufferManager() {

  }

  /**
   *
   * @param readAheadBlockSize
   * @param thresholdAgeMilliseconds
   */
  @Override
  public void testResetReadBufferManager(final int readAheadBlockSize,
      final int thresholdAgeMilliseconds) {

  }

  /**
   *
   * @param thresholdAgeMs
   */
  @Override
  public void setThresholdAgeMilliseconds(final int thresholdAgeMs) {

  }

  /**
   *
   * @return
   */
  @Override
  public int getThresholdAgeMilliseconds() {
    return 0;
  }

  /**
   *
   * @return
   */
  @Override
  public int getCompletedReadListSize() {
    return 0;
  }

  /**
   *
   */
  @Override
  public void callTryEvict() {

  }

  /**
   *
   * @param buf
   */
  @Override
  public void testMimicFullUseAndAddFailedBuffer(final ReadBuffer buf) {

  }

  /**
   *
   * @return
   */
  @Override
  public int getNumBuffers() {
    return 0;
  }

  /**
   *
   * @return
   */
  @Override
  public List<ReadBuffer> getInProgressCopiedList() {
    return Collections.emptyList();
  }

  /**
   *
   * @return
   */
  @Override
  public List<ReadBuffer> getCompletedReadListCopy() {
    return Collections.emptyList();
  }

  /**
   *
   * @return
   */
  @Override
  public List<Integer> getFreeListCopy() {
    return Collections.emptyList();
  }

  /**
   *
   * @return
   */
  @Override
  public int getReadAheadBlockSize() {
    return 0;
  }
}
