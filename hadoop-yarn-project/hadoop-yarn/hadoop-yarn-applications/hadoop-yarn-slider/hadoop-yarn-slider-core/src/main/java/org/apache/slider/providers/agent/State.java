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
public enum State {
  INIT,           // Not installed
  INSTALLING,     // Being installed
  INSTALLED,      // Installed (or stopped)
  STARTING,       // Starting
  STARTED,        // Started
  INSTALL_FAILED, // Install failed, start failure in INSTALLED
  UPGRADING,      // Undergoing upgrade, perform necessary pre-upgrade steps
  UPGRADED,       // Pre-upgrade steps completed
  STOPPING,       // Stop has been issued
  STOPPED,        // Agent has stopped
  TERMINATING;    // Terminate signal to ask the agent to kill itself
                  // No need for state TERMINATED (as the agent is dead by then)

  /**
   * Indicates whether or not it is a valid state to produce a command.
   *
   * @return true if command can be issued for this state.
   */
  public boolean canIssueCommands() {
    switch (this) {
      case INSTALLING:
      case STARTING:
      case UPGRADING:
      case STOPPING:
      case TERMINATING:
        return false;
      default:
        return true;
    }
  }

  /**
   * Returns valid command in this state.
   *
   * @return command allowed in this state.
   */
  public Command getSupportedCommand() {
    return getSupportedCommand(false);
  }

  public Command getSupportedCommand(boolean isInUpgradeMode) {
    return getSupportedCommand(isInUpgradeMode, false);
  }

  public Command getSupportedCommand(boolean isInUpgradeMode,
      boolean stopInitiated) {
    switch (this) {
      case INIT:
      case INSTALL_FAILED:
        return Command.INSTALL;
      case INSTALLED:
        return Command.START;
      case STARTED:
      return isInUpgradeMode ? Command.UPGRADE : (stopInitiated) ? Command.STOP
          : Command.NOP;
      case UPGRADED:
        return Command.STOP;
      case STOPPED:
        return Command.TERMINATE;
      default:
        return Command.NOP;
    }
  }

  /**
   * Returns next state based on the command result.
   *
   * @return next state.
   */
  public State getNextState(CommandResult result) throws IllegalArgumentException {
    switch (result) {
      case IN_PROGRESS:
        if (this == State.INSTALLING || this == State.STARTING
            || this == State.UPGRADING || this == State.STOPPING
            || this == State.TERMINATING) {
          return this;
        } else {
          throw new IllegalArgumentException(result + " is not valid for " + this);
        }
      case COMPLETED:
        if (this == State.INSTALLING) {
          return State.INSTALLED;
        } else if (this == State.STARTING) {
          return State.STARTED;
        } else if (this == State.UPGRADING) {
          return State.UPGRADED;
        } else if (this == State.STOPPING) {
          return State.STOPPED;
        } else {
          throw new IllegalArgumentException(result + " is not valid for " + this);
        }
      case FAILED:
        if (this == State.INSTALLING) {
          return State.INSTALL_FAILED;
        } else if (this == State.STARTING) {
          return State.INSTALLED;
        } else if (this == State.UPGRADING) {
          // if pre-upgrade failed, force stop now, so mark it upgraded
          // what other options can be exposed to app owner?
          return State.UPGRADED;
        } else if (this == State.STOPPING) {
          // if stop fails, force mark it stopped (and let container terminate)
          return State.STOPPED;
        } else if (this == State.STOPPED) {
          // if in stopped state, force mark it as terminating
          return State.TERMINATING;
        } else {
          throw new IllegalArgumentException(result + " is not valid for " + this);
        }
      default:
        throw new IllegalArgumentException("Bad command result " + result);
    }
  }

  /**
   * Returns next state based on the command.
   *
   * @return next state.
   */
  public State getNextState(Command command) throws IllegalArgumentException {
    switch (command) {
      case INSTALL:
        if (this == State.INIT || this == State.INSTALL_FAILED) {
          return State.INSTALLING;
        } else {
          throw new IllegalArgumentException(command + " is not valid for " + this);
        }
      case INSTALL_ADDON:
          if (this == State.INIT || this == State.INSTALL_FAILED) {
            return State.INSTALLING;
          } else {
            throw new IllegalArgumentException(command + " is not valid for " + this);
          }
      case START:
        if (this == State.INSTALLED) {
          return State.STARTING;
        } else {
          throw new IllegalArgumentException(command + " is not valid for " + this);
        }
      case UPGRADE:
        if (this == State.STARTED) {
          return State.UPGRADING;
        } else {
          throw new IllegalArgumentException(command + " is not valid for " + this);
        }
      case STOP:
        if (this == State.STARTED || this == State.UPGRADED) {
          return State.STOPPING;
        } else {
          throw new IllegalArgumentException(command + " is not valid for " + this);
        }
      case TERMINATE:
        if (this == State.STOPPED) {
          return State.TERMINATING;
        } else {
          throw new IllegalArgumentException(command + " is not valid for " + this);
        }
      case NOP:
        return this;
      default:
        throw new IllegalArgumentException("Bad command " + command);
    }
  }

  public boolean couldHaveIssued(Command command) {
    if ((this == State.INSTALLING && command == Command.INSTALL)
        || (this == State.STARTING && command == Command.START)
        || (this == State.UPGRADING && command == Command.UPGRADE)
        || (this == State.STOPPING 
           && (command == Command.STOP || command == Command.NOP))
        || (this == State.TERMINATING && command == Command.TERMINATE)
       ) {
      return true;
    }
    return false;
  }
}
