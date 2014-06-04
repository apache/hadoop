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


/**
 * Common Exit codes.
 * 
 * Codes with a YARN prefix are YARN-related.
 * Exit codes from 32 up are defined for the {@link ServiceLauncher}. 
 * These are away up from the the base numbers, to distinguish them. 
 * Applications can have their own set of failures -it is recommended to
 * start them at 64 to differentiate them.
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
   * YARN Exit code on a client initiated AM termination: {@value}
   */
  int EXIT_CLIENT_INITIATED_SHUTDOWN  =  1;
  
  /**
   * YARN Exit code when tasks could not be launched: {@value}
   */
  int EXIT_TASK_LAUNCH_FAILURE        =  2;
  
  /**
   * Exit code when an exception was thrown from the service: {@value}
   */
  int EXIT_EXCEPTION_THROWN           = 32;
  
  /**
   * Exit code when a usage message was printed: {@value}
   */
  int EXIT_USAGE                      = 33;
  
  /**
   * Exit code when something happened but we can't be specific: {@value}
   */
  int EXIT_OTHER_FAILURE              = 34;
  
  /**
   * Exit code when a control-C, kill -3, signal was picked up: {@value}
   */
                                
  int EXIT_INTERRUPTED                = 35;
  
  /**
   * Exit code when the command line doesn't parse: {@value}, or
   * when it is otherwise invalid.
   */
  int EXIT_COMMAND_ARGUMENT_ERROR     = 36;
  
  /**
   * Exit code when the configurations in valid/incomplete: {@value}
   */
  int EXIT_BAD_CONFIGURATION          = 37;
 
  /**
   * Exit code on network connectivity problems: {@value}
   */
  int EXIT_CONNECTIVITY_PROBLEM       = 38;
 
  
}
