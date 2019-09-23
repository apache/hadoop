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

import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.model.CopyResult;

/**
 * Extracts the outcome of a TransferManager-executed copy operation.
 */
public final class CopyOutcome {

  /**
   * Result of a successful copy.
   */
  private final CopyResult copyResult;

  /** the copy was interrupted. */
  private final InterruptedException interruptedException;

  /**
   * The copy raised an AWS Exception of some form.
   */
  private final SdkBaseException awsException;

  public CopyOutcome(CopyResult copyResult,
      InterruptedException interruptedException,
      SdkBaseException awsException) {
    this.copyResult = copyResult;
    this.interruptedException = interruptedException;
    this.awsException = awsException;
  }

  public CopyResult getCopyResult() {
    return copyResult;
  }

  public InterruptedException getInterruptedException() {
    return interruptedException;
  }

  public SdkBaseException getAwsException() {
    return awsException;
  }

  /**
   * Calls {@code Copy.waitForCopyResult()} to await the result, converts
   * it to a copy outcome.
   * Exceptions caught and
   * @param copy the copy operation.
   * @return the outcome.
   */
  public static CopyOutcome waitForCopy(Copy copy) {
    try {
      CopyResult result = copy.waitForCopyResult();
      return new CopyOutcome(result, null, null);
    } catch (SdkBaseException e) {
      return new CopyOutcome(null, null, e);
    } catch (InterruptedException e) {
      return new CopyOutcome(null, e, null);
    }
  }
}
