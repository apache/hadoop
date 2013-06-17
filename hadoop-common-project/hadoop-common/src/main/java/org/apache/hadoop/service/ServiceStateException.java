/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.service;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Exception that is raised on state change operations.
 */
@Public
@Evolving
public class ServiceStateException extends RuntimeException {

  private static final long serialVersionUID = 1110000352259232646L;

  public ServiceStateException(String message) {
    super(message);
  }

  public ServiceStateException(String message, Throwable cause) {
    super(message, cause);
  }

  public ServiceStateException(Throwable cause) {
    super(cause);
  }

  /**
   * Convert any exception into a {@link RuntimeException}.
   * If the caught exception is already of that type, it is typecast to a
   * {@link RuntimeException} and returned.
   *
   * All other exception types are wrapped in a new instance of
   * ServiceStateException
   * @param fault exception or throwable
   * @return a ServiceStateException to rethrow
   */
  public static RuntimeException convert(Throwable fault) {
    if (fault instanceof RuntimeException) {
      return (RuntimeException) fault;
    } else {
      return new ServiceStateException(fault);
    }
  }

  /**
   * Convert any exception into a {@link RuntimeException}.
   * If the caught exception is already of that type, it is typecast to a
   * {@link RuntimeException} and returned.
   *
   * All other exception types are wrapped in a new instance of
   * ServiceStateException
   * @param text text to use if a new exception is created
   * @param fault exception or throwable
   * @return a ServiceStateException to rethrow
   */
  public static RuntimeException convert(String text, Throwable fault) {
    if (fault instanceof RuntimeException) {
      return (RuntimeException) fault;
    } else {
      return new ServiceStateException(text, fault);
    }
  }
}
