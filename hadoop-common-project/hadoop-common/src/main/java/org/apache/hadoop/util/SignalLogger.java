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

package org.apache.hadoop.util;

import org.slf4j.Logger;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class logs a message whenever we're about to exit on a UNIX signal.
 * This is helpful for determining the root cause of a process' exit.
 * For example, if the process exited because the system administrator 
 * ran a standard "kill," you would see 'EXITING ON SIGNAL SIGTERM' in the log.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum SignalLogger {
  INSTANCE;

  private boolean registered = false;

  /**
   * Our signal handler.
   */
  private static class Handler implements SignalHandler {
    final private Logger log;
    final private SignalHandler prevHandler;

    Handler(String name, Logger log) {
      this.log = log;
      prevHandler = Signal.handle(new Signal(name), this);
    }

    /**
     * Handle an incoming signal.
     *
     * @param signal    The incoming signal
     */
    @Override
    public void handle(Signal signal) {
      log.error("RECEIVED SIGNAL " + signal.getNumber() +
          ": SIG" + signal.getName());
      prevHandler.handle(signal);
    }
  }

  /**
   * Register some signal handlers.
   *
   * @param log The log4j logfile to use in the signal handlers.
   */
  public void register(final Logger log) {
    if (registered) {
      throw new IllegalStateException("Can't re-install the signal handlers.");
    }
    registered = true;
    StringBuilder bld = new StringBuilder();
    bld.append("registered UNIX signal handlers for [");
    final String SIGNALS[] = { "TERM", "HUP", "INT" };
    String separator = "";
    for (String signalName : SIGNALS) {
      try {
        new Handler(signalName, log);
        bld.append(separator)
            .append(signalName);
        separator = ", ";
      } catch (Exception e) {
        log.debug("Error: ", e);
      }
    }
    bld.append("]");
    log.info(bld.toString());
  }
}
