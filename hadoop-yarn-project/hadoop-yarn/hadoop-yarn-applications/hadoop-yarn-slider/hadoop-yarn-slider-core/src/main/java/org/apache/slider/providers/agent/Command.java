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

/** The states a component instance can be. */
public enum Command {
  NOP,           // do nothing
  INSTALL,       // Install the component
  INSTALL_ADDON, // Install add on packages if any
  START,         // Start the component
  STOP,          // Stop the component
  UPGRADE,       // The component will undergo upgrade
  TERMINATE;     // Send terminate signal to agent

  public static Command getCommand(String commandVal) {
    if (commandVal.equals(Command.START.toString())) {
      return Command.START;
    }
    if (commandVal.equals(Command.INSTALL.toString())) {
      return Command.INSTALL;
    }
    if (commandVal.equals(Command.STOP.toString())) {
      return Command.STOP;
    }
    if (commandVal.equals(Command.UPGRADE.toString())) {
      return Command.UPGRADE;
    }
    if (commandVal.equals(Command.TERMINATE.toString())) {
      return Command.TERMINATE;
    }

    return Command.NOP;
  }

  public static String transform(Command command, boolean isUpgrade) {
    switch (command) {
    case STOP:
      return isUpgrade ? "UPGRADE_STOP" : command.name();
    default:
      return command.name();
    }
  }
}
