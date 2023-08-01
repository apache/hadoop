/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnException;

/** Exception caused in a container runtime impl. 'Runtime' is not used in
 * the class name to avoid confusion with a java RuntimeException
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ContainerExecutionException extends YarnException {
  private static final long serialVersionUID = 1L;
  private static final int EXIT_CODE_UNSET = -1;
  private static final String OUTPUT_UNSET = "<unknown>";

  private int exitCode;
  private String output;
  private String errorOutput;

  public ContainerExecutionException(String message) {
    super(message);
    exitCode = EXIT_CODE_UNSET;
    output = OUTPUT_UNSET;
    errorOutput = OUTPUT_UNSET;
  }

  public ContainerExecutionException(Throwable throwable) {
    super(throwable);
    exitCode = EXIT_CODE_UNSET;
    output = OUTPUT_UNSET;
    errorOutput = OUTPUT_UNSET;
  }

  public ContainerExecutionException(String message, int exitCode) {
    super(message);
    this.exitCode = exitCode;
    this.output = OUTPUT_UNSET;
    this.errorOutput = OUTPUT_UNSET;
  }

  public ContainerExecutionException(String message, int exitCode, String
      output, String errorOutput) {
    super(message);
    this.exitCode = exitCode;
    this.output = output;
    this.errorOutput = errorOutput;
  }

  public ContainerExecutionException(Throwable cause, int exitCode, String
      output, String errorOutput) {
    super(cause);
    this.exitCode = exitCode;
    this.output = output;
    this.errorOutput = errorOutput;
  }

  public int getExitCode() {
    return exitCode;
  }

  public String getOutput() {
    return output;
  }

  public String getErrorOutput() {
    return errorOutput;
  }

  public static int getDefaultExitCode() {
    return EXIT_CODE_UNSET;
  }

}