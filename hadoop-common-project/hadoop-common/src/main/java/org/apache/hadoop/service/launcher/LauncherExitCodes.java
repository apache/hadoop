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

package org.apache.hadoop.service.launcher;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Common Exit codes.
 * <p>
 * Codes with a YARN prefix are YARN-related.
 * <p>
 * Many of the exit codes are designed to resemble HTTP error codes,
 * squashed into a single byte. e.g 44 , "not found" is the equivalent
 * of 404. The various 2XX HTTP error codes aren't followed;
 * the Unix standard of "0" for success is used.
 * <pre>
 *    0-10: general command issues
 *   30-39: equivalent to the 3XX responses, where those responses are
 *          considered errors by the application.
 *   40-49: client-side/CLI/config problems
 *   50-59: service-side problems.
 *   60+  : application specific error codes
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface LauncherExitCodes {

  /**
   * Success: {@value}.
   */
  int EXIT_SUCCESS = 0;

  /**
   * Generic "false/fail" response: {@value}.
   * The operation worked but the result was not "true" from the viewpoint
   * of the executed code.
   */
  int EXIT_FAIL = -1;

  /**
   * Exit code when a client requested service termination: {@value}.
   */
  int EXIT_CLIENT_INITIATED_SHUTDOWN = 1;

  /**
   * Exit code when targets could not be launched: {@value}.
   */
  int EXIT_TASK_LAUNCH_FAILURE = 2;

  /**
   * Exit code when a control-C, kill -3, signal was picked up: {@value}.
   */
  int EXIT_INTERRUPTED = 3;

  /**
   * Exit code when something happened but we can't be specific: {@value}.
   */
  int EXIT_OTHER_FAILURE = 5;

  /**
   * Exit code when the command line doesn't parse: {@value}, or
   * when it is otherwise invalid.
   * <p>
   * Approximate HTTP equivalent: {@code 400 Bad Request}
   */
  int EXIT_COMMAND_ARGUMENT_ERROR = 40;

  /**
   * The request requires user authentication: {@value}.
   * <p>
   * approximate HTTP equivalent: Approximate HTTP equivalent: {@code 401 Unauthorized}
   */
  int EXIT_UNAUTHORIZED = 41;

  /**
   * Exit code when a usage message was printed: {@value}.
   */
  int EXIT_USAGE = 42;

  /**
   * Forbidden action: {@value}.
   * <p>
   * Approximate HTTP equivalent: Approximate HTTP equivalent: {@code 403: Forbidden}
   */
  int EXIT_FORBIDDEN = 43;

  /**
   * Something was not found: {@value}.
   * <p>
   * Approximate HTTP equivalent: {@code 404: Not Found}
   */
  int EXIT_NOT_FOUND = 44;

  /**
   * The operation is not allowed: {@value}.
   * <p>
   * Approximate HTTP equivalent: {@code 405: Not allowed}
   */
  int EXIT_OPERATION_NOT_ALLOWED = 45;

  /**
   * The command is somehow not acceptable: {@value}.
   * <p>
   * Approximate HTTP equivalent: {@code 406: Not Acceptable}
   */
  int EXIT_NOT_ACCEPTABLE = 46;

  /**
   * Exit code on connectivity problems: {@value}.
   * <p>
   * Approximate HTTP equivalent: {@code 408: Request Timeout}
   */
  int EXIT_CONNECTIVITY_PROBLEM = 48;

  /**
   * Exit code when the configurations in valid/incomplete: {@value}.
   * <p>
   * Approximate HTTP equivalent: {@code 409: Conflict}
   */
  int EXIT_BAD_CONFIGURATION = 49;

  /**
   * Exit code when an exception was thrown from the service: {@value}.
   * <p>
   * Approximate HTTP equivalent: {@code 500 Internal Server Error}
   */
  int EXIT_EXCEPTION_THROWN = 50;

  /**
   * Unimplemented feature: {@value}.
   * <p>
   * Approximate HTTP equivalent: {@code 501: Not Implemented}
   */
  int EXIT_UNIMPLEMENTED = 51;

  /**
   * Service Unavailable; it may be available later: {@value}.
   * <p>
   * Approximate HTTP equivalent: {@code 503 Service Unavailable}
   */
  int EXIT_SERVICE_UNAVAILABLE = 53;

  /**
   * The application does not support, or refuses to support this
   * version: {@value}.
   * <p>
   * If raised, this is expected to be raised server-side and likely due
   * to client/server version incompatibilities.
   * <p>
   * Approximate HTTP equivalent: {@code 505: Version Not Supported}
   */
  int EXIT_UNSUPPORTED_VERSION = 55;

  /**
   * The service instance could not be created: {@value}.
   */
  int EXIT_SERVICE_CREATION_FAILURE = 56;
 
  /**
   * The service instance could not be created: {@value}.
   */
  int EXIT_SERVICE_LIFECYCLE_EXCEPTION = 57;
  
}
