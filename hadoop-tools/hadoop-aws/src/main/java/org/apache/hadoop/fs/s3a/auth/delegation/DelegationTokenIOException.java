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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.IOException;

/**
 * General IOException for Delegation Token issues.
 * Includes recommended error strings, which can be used in tests when
 * looking for specific errors.
 */
public class DelegationTokenIOException extends IOException {

  private static final long serialVersionUID = 599813827985340023L;

  /** Error: delegation token/token identifier class isn't the right one. */
  public static final String TOKEN_WRONG_CLASS
      = "Delegation token is wrong class";

  /**
   * The far end is expecting a different token kind than
   * that which the client created.
   */
  protected static final String TOKEN_MISMATCH = "Token mismatch";

  public DelegationTokenIOException(final String message) {
    super(message);
  }

  public DelegationTokenIOException(final String message,
      final Throwable cause) {
    super(message, cause);
  }
}
