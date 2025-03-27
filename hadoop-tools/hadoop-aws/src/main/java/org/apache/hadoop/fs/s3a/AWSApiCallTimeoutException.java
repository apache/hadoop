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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.net.ConnectTimeoutException;

/**
 * IOException equivalent of an {@code ApiCallTimeoutException}.
 * Declared as a subclass of {@link ConnectTimeoutException} to allow
 * for existing code to catch it.
 */
public class AWSApiCallTimeoutException extends ConnectTimeoutException {

  /**
   * Constructor.
   * @param operation operation in progress
   * @param cause cause.
   */
  public AWSApiCallTimeoutException(
      final String operation,
      final Exception cause) {
    super(operation);
    initCause(cause);
  }
}
