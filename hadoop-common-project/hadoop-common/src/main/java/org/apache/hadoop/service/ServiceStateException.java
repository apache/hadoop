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
 * Exception that is raised on state change operations.
 */
@Public
@Evolving
public class ServiceStateException extends RuntimeException implements
    ExitCodeProvider {

  private static final long serialVersionUID = 1110000352259232646L;
  /**
   * Exit code. Non final as the constructor logic is too complex for
   * the compiler to like.
   */
  private int exitCode;

  public ServiceStateException(String message) {
    this(message, null);
  }

  public ServiceStateException(String message, Throwable cause) {
    this(LauncherExitCodes.EXIT_SERVICE_LIFECYCLE_EXCEPTION, message, cause);
  }

  private ServiceStateException(int exitCode,
      String message,
      Throwable cause) {
    super(message, cause);

    this.exitCode = (cause instanceof ExitCodeProvider)?
                    (((ExitCodeProvider) cause).getExitCode())
                   : exitCode;
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
