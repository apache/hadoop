/*
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

package org.apache.slider.providers.agent;

/** Command results. */
public enum CommandResult {
  IN_PROGRESS,  // Command is in progress
  COMPLETED,    // Command has successfully completed
  FAILED;        // Command has failed

  public static CommandResult getCommandResult(String commandResVal) {
    if (commandResVal.equals(CommandResult.COMPLETED.toString())) {
      return CommandResult.COMPLETED;
    }
    if (commandResVal.equals(CommandResult.FAILED.toString())) {
      return CommandResult.FAILED;
    }
    if (commandResVal.equals(CommandResult.IN_PROGRESS.toString())) {
      return CommandResult.IN_PROGRESS;
    }

    throw new IllegalArgumentException("Unrecognized value " + commandResVal);
  }
}
