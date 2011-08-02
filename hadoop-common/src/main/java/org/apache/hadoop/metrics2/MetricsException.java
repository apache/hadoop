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

package org.apache.hadoop.metrics2;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A general metrics exception wrapper
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /**
   * Construct the exception with a message
   * @param message for the exception
   */
  public MetricsException(String message) {
    super(message);
  }

  /**
   * Construct the exception with a message and a cause
   * @param message for the exception
   * @param cause of the exception
   */
  public MetricsException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Construct the exception with a cause
   * @param cause of the exception
   */
  public MetricsException(Throwable cause) {
    super(cause);
  }
}
