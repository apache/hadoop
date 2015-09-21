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

package org.apache.hadoop.tools.util;

/**
 *  WorkReport{@literal <T>} is a simple container for items of class T and its
 *  corresponding retry counter that indicates how many times this item
 *  was previously attempted to be processed.
 */
public class WorkReport<T> {
  private T item;
  private final boolean success;
  private final int retry;
  private final Exception exception;

  /**
   *  @param  item       Object representing work report.
   *  @param  retry      Number of unsuccessful attempts to process work.
   *  @param  success    Indicates whether work was successfully completed.
   */
  public WorkReport(T item, int retry, boolean success) {
    this(item, retry, success, null);
  }

  /**
   *  @param  item       Object representing work report.
   *  @param  retry      Number of unsuccessful attempts to process work.
   *  @param  success    Indicates whether work was successfully completed.
   *  @param  exception  Exception thrown while processing work.
   */
  public WorkReport(T item, int retry, boolean success, Exception exception) {
    this.item = item;
    this.retry = retry;
    this.success = success;
    this.exception = exception;
  }

  public T getItem() {
    return item;
  }

  /**
   *  @return True if the work was processed successfully.
   */
  public boolean getSuccess() {
    return success;
  }

  /**
   *  @return  Number of unsuccessful attempts to process work.
   */
  public int getRetry() {
    return retry;
  }

  /**
   *  @return  Exception thrown while processing work.
   */
  public Exception getException() {
    return exception;
  }
}
