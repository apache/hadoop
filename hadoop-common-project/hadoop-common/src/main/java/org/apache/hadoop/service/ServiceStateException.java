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
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.util.ExitCodeProvider;

/**
 * Exception that can be raised on state change operations, whose
 * exit code can be explicitly set, determined from that of any nested
 * cause, or a default value of
 * {@link  LauncherExitCodes#EXIT_SERVICE_LIFECYCLE_EXCEPTION}.
 */
@Public
@Evolving
public class ServiceStateException extends RuntimeException implements
    ExitCodeProvider {

  private static final long serialVersionUID = 1110000352259232646L;

  /**
   * Exit code.
   */
  private int exitCode ;

  /**
   * Instantiate
   * @param message error message
   */
  public ServiceStateException(String message) {
    this(message, null);
  }

  /**
   * Instantiate with a message and cause; if the cause has an exit code
   * then it is used, otherwise the generic
   * {@link LauncherExitCodes#EXIT_SERVICE_LIFECYCLE_EXCEPTION} exit code
   * is used.
   * @param message exception message
   * @param cause optional inner cause
   */
  public ServiceStateException(String message, Throwable cause) {
    super(message, cause);
    if(cause instanceof ExitCodeProvider) {
      this.exitCode = ((ExitCodeProvider) cause).getExitCode();
    } else {
      this.exitCode = LauncherExitCodes.EXIT_SERVICE_LIFECYCLE_EXCEPTION;
    }
  }

  /**
   * Instantiate, using the specified exit code as the exit code
   * of the exception, irrespetive of any exit code supplied by any inner
   * cause.
   *
   * @param exitCode exit code to declare
   * @param message exception message
   * @param cause inner cause
   */
  public ServiceStateException(int exitCode,
      String message,
      Throwable cause) {
    this(message, cause);
    this.exitCode =  exitCode;
  }

  public ServiceStateException(Throwable cause) {
    super(cause);
  }

  @Override
  public int getExitCode() {
    return exitCode;
  }

  /**
   * Convert any exception into a {@link RuntimeException}.
   * All other exception types are wrapped in a new instance of
   * {@code ServiceStateException}.
   * @param fault exception or throwable
   * @return a {@link RuntimeException} to rethrow
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
   * {@code ServiceStateException}.
   * @param text text to use if a new exception is created
   * @param fault exception or throwable
   * @return a {@link RuntimeException} to rethrow
   */
  public static RuntimeException convert(String text, Throwable fault) {
    if (fault instanceof RuntimeException) {
      return (RuntimeException) fault;
    } else {
      return new ServiceStateException(text, fault);
    }
  }
}
