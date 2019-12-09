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

package org.apache.hadoop.yarn.service.exceptions;

/*
 * Common Exit codes
 * <p>
 * Exit codes from 64 up are service specific.
 * <p>
 * Many of the exit codes are designed to resemble HTTP error codes,
 * squashed into a single byte. e.g 44 , "not found" is the equivalent
 * of 404
 * <pre>
 *    0-10: general command issues
 *   30-39: equivalent to the 3XX responses, where those responses are
 *          considered errors by the service.
 *   40-49: request-related errors
 *   50-59: server-side problems. These may be triggered by the request.
 *   64-  : service specific error codes
 * </pre>
 */
public interface LauncherExitCodes {
  
  /**
   * 0: success
   */
  int EXIT_SUCCESS                    =  0;

  /**
   * -1: generic "false" response. The operation worked but
   * the result was not true
   */
  int EXIT_FALSE                      = -1;

  /**
   * Exit code when a client requested service termination: {@value}
   */
  int EXIT_CLIENT_INITIATED_SHUTDOWN  =  1;

  /**
   * Exit code when targets could not be launched: {@value}
   */
  int EXIT_TASK_LAUNCH_FAILURE        =  2;

  /**
   * Exit code when a control-C, kill -3, signal was picked up: {@value}
   */
  int EXIT_INTERRUPTED                = 3;

  /**
   * Exit code when a usage message was printed: {@value}
   */
  int EXIT_USAGE                      = 4;

  /**
   * Exit code when something happened but we can't be specific: {@value}
   */
  int EXIT_OTHER_FAILURE               = 5;

  /**
   * Exit code on connectivity problems: {@value}
   */
  int EXIT_MOVED                      = 31;
  
  /**
   * found: {@value}.
   * <p>
   * This is low value as in HTTP it is normally a success/redirect;
   * whereas on the command line 0 is the sole success code.
   * <p>
   * <code>302 Found</code>
   */
  int EXIT_FOUND                      = 32;

  /**
   * Exit code on a request where the destination has not changed
   * and (somehow) the command specified that this is an error.
   * That is, this exit code is somehow different from a "success"
   * : {@value}
   * <p>
   * <code>304 Not Modified </code>
  */
  int EXIT_NOT_MODIFIED               = 34;

  /**
   * Exit code when the command line doesn't parse: {@value}, or
   * when it is otherwise invalid.
   * <p>
   * <code>400 BAD REQUEST</code>
   */
  int EXIT_COMMAND_ARGUMENT_ERROR     = 40;

  /**
   * The request requires user authentication: {@value}
   * <p>
   * <code>401 Unauthorized</code>
   */
  int EXIT_UNAUTHORIZED               = 41;
  
  /**
   * Forbidden action: {@value}
   * <p>
   * <code>403: Forbidden</code>
   */
  int EXIT_FORBIDDEN                  = 43;
  
  /**
   * Something was not found: {@value}
   * <p>
   * <code>404: NOT FOUND</code>
   */
  int EXIT_NOT_FOUND                  = 44;

  /**
   * The operation is not allowed: {@value}
   * <p>
   * <code>405: NOT ALLOWED</code>
   */
  int EXIT_OPERATION_NOT_ALLOWED       = 45;

  /**
   * The command is somehow not acceptable: {@value}
   * <p>
   * <code>406: NOT ACCEPTABLE</code>
   */
  int EXIT_NOT_ACCEPTABLE            = 46;

  /**
   * Exit code on connectivity problems: {@value}
   * <p>
   * <code>408: Request Timeout</code>
   */
  int EXIT_CONNECTIVITY_PROBLEM       = 48;

  /**
   * The request could not be completed due to a conflict with the current
   * state of the resource.  {@value}
   * <p>
   * <code>409: conflict</code>
   */
  int EXIT_CONFLICT                   = 49;

  /**
   * internal error: {@value}
   * <p>
   * <code>500 Internal Server Error</code>
   */
  int EXIT_INTERNAL_ERROR             = 50;

  /**
   * Unimplemented feature: {@value}
   * <p>
   * <code>501: Not Implemented</code>
   */
  int EXIT_UNIMPLEMENTED              = 51;

  /**
   * Service Unavailable; it may be available later: {@value}
   * <p>
   * <code>503 Service Unavailable</code>
   */
  int EXIT_SERVICE_UNAVAILABLE        = 53;

  /**
   * The service does not support, or refuses to support this version: {@value}.
   * If raised, this is expected to be raised server-side and likely due
   * to client/server version incompatibilities.
   * <p>
   * <code> 505: Version Not Supported</code>
   */
  int EXIT_UNSUPPORTED_VERSION        = 55;

  /**
   * Exit code when an exception was thrown from the service: {@value}
   * <p>
   * <code>5XX</code>
   */
  int EXIT_EXCEPTION_THROWN           = 56;

}
