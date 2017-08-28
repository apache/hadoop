/*
 *  Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.yarn.service.exceptions;


import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * A service launch exception that includes an exit code;
 * when caught by the ServiceLauncher, it will convert that
 * into a process exit code.
 */
public class ServiceLaunchException extends YarnException
  implements ExitCodeProvider, LauncherExitCodes {

  private final int exitCode;

  /**
   * Create an exception with the specific exit code
   * @param exitCode exit code
   * @param cause cause of the exception
   */
  public ServiceLaunchException(int exitCode, Throwable cause) {
    super(cause);
    this.exitCode = exitCode;
  }

  /**
   * Create an exception with the specific exit code and text
   * @param exitCode exit code
   * @param message message to use in exception
   */
  public ServiceLaunchException(int exitCode, String message) {
    super(message);
    this.exitCode = exitCode;
  }

  /**
   * Create an exception with the specific exit code, text and cause
   * @param exitCode exit code
   * @param message message to use in exception
   * @param cause cause of the exception
   */
  public ServiceLaunchException(int exitCode, String message, Throwable cause) {
    super(message, cause);
    this.exitCode = exitCode;
  }

  /**
   * Get the exit code
   * @return the exit code
   */
  @Override
  public int getExitCode() {
    return exitCode;
  }
}
