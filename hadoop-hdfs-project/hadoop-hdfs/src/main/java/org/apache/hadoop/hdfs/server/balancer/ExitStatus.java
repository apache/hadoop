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
package org.apache.hadoop.hdfs.server.balancer;

/**
 * Exit status - The values associated with each exit status is directly mapped
 * to the process's exit code in command line.
 */
public enum ExitStatus {
  SUCCESS(0),
  IN_PROGRESS(1),
  ALREADY_RUNNING(-1),
  NO_MOVE_BLOCK(-2),
  NO_MOVE_PROGRESS(-3),
  IO_EXCEPTION(-4),
  ILLEGAL_ARGUMENTS(-5),
  INTERRUPTED(-6);

  private final int code;

  private ExitStatus(int code) {
    this.code = code;
  }
  
  /** @return the command line exit code. */
  public int getExitCode() {
    return code;
  }
}