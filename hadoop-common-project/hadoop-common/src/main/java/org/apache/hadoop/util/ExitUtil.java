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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Facilitates hooking process termination for tests and debugging.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public final class ExitUtil {
  private final static Log LOG = LogFactory.getLog(ExitUtil.class.getName());
  private static volatile boolean systemExitDisabled = false;
  private static volatile boolean terminateCalled = false;

  public static class ExitException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public final int status;

    public ExitException(int status, String msg) {
      super(msg);
      this.status = status;
    }
  }

  /**
   * Disable the use of System.exit for testing.
   */
  public static void disableSystemExit() {
    systemExitDisabled = true;
  }

  /**
   * Clear the previous exit record.
   */
  public static void clearTerminateCalled() {
    terminateCalled = false;
  }

  /**
   * @return true if terminate has been called
   */
  public static boolean terminateCalled() {
    return terminateCalled;
  }

  /**
   * Terminate the current process. Note that terminate is the *only* method
   * that should be used to terminate the daemon processes.
   * @param status exit code
   * @param msg message used to create the ExitException
   * @throws ExitException if System.exit is disabled for test purposes
   */
  public static void terminate(int status, String msg) throws ExitException {
    LOG.info("Exiting with status " + status);
    terminateCalled = true;
    if (systemExitDisabled) {
      throw new ExitException(status, msg);
    }
    System.exit(status);
  }

  /**
   * Like {@link terminate(int, String)} without a message.
   * @param status
   * @throws ExitException
   */
  public static void terminate(int status) throws ExitException {
    terminate(status, "ExitException");
  }
}
