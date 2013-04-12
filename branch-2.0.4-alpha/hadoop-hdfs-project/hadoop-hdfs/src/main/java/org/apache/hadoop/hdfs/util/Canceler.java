/**
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
package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Provides a simple interface where one thread can mark an operation
 * for cancellation, and another thread can poll for whether the
 * cancellation has occurred.
 */
@InterfaceAudience.Private
public class Canceler {
  /**
   * If the operation has been canceled, set to the reason why
   * it has been canceled (eg standby moving to active)
   */
  private volatile String cancelReason = null;
  
  /**
   * Requests that the current operation be canceled if it is still running.
   * This does not block until the cancellation is successful.
   * @param reason the reason why cancellation is requested
   */
  public void cancel(String reason) {
    this.cancelReason = reason;
  }

  public boolean isCancelled() {
    return cancelReason != null;
  }
  
  public String getCancellationReason() {
    return cancelReason;
  }
}
