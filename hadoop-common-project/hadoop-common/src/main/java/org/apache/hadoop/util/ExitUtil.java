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
  private static volatile boolean systemHaltDisabled = false;
  private static volatile ExitException firstExitException;
  private static volatile HaltException firstHaltException;

  public static class ExitException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public final int status;

    public ExitException(int status, String msg) {
      super(msg);
      this.status = status;
    }
  }

  public static class HaltException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public final int status;

    public HaltException(int status, String msg) {
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
   * Disable the use of {@code Runtime.getRuntime().halt() } for testing.
   */
  public static void disableSystemHalt() {
    systemHaltDisabled = true;
  }

  /**
   * @return true if terminate has been called
   */
  public static boolean terminateCalled() {
    // Either we set this member or we actually called System#exit
    return firstExitException != null;
  }

  /**
   * @return true if halt has been called
   */
  public static boolean haltCalled() {
    return firstHaltException != null;
  }

  /**
   * @return the first ExitException thrown, null if none thrown yet
   */
  public static ExitException getFirstExitException() {
    return firstExitException;
  }

  /**
   * @return the first {@code HaltException} thrown, null if none thrown yet
   */
  public static HaltException getFirstHaltException() {
    return firstHaltException;
  }

  /**
   * Reset the tracking of process termination. This is for use in unit tests
   * where one test in the suite expects an exit but others do not.
   */
  public static void resetFirstExitException() {
    firstExitException = null;
  }

  public static void resetFirstHaltException() {
    firstHaltException = null;
  }

  /**
   * Terminate the current process. Note that terminate is the *only* method
   * that should be used to terminate the daemon processes.
   *
   * @param status
   *          exit code
   * @param msg
   *          message used to create the {@code ExitException}
   * @throws ExitException
   *           if System.exit is disabled for test purposes
   */
  public static void terminate(int status, String msg) throws ExitException {
    LOG.info("Exiting with status " + status);
    if (systemExitDisabled) {
      ExitException ee = new ExitException(status, msg);
      LOG.fatal("Terminate called", ee);
      if (null == firstExitException) {
        firstExitException = ee;
      }
      throw ee;
    }
    System.exit(status);
  }

  /**
   * Forcibly terminates the currently running Java virtual machine.
   *
   * @param status
   *          exit code
   * @param msg
   *          message used to create the {@code HaltException}
   * @throws HaltException
   *           if Runtime.getRuntime().halt() is disabled for test purposes
   */
  public static void halt(int status, String msg) throws HaltException {
    LOG.info("Halt with status " + status + " Message: " + msg);
    if (systemHaltDisabled) {
      HaltException ee = new HaltException(status, msg);
      LOG.fatal("Halt called", ee);
      if (null == firstHaltException) {
        firstHaltException = ee;
      }
      throw ee;
    }
    Runtime.getRuntime().halt(status);
  }

  /**
   * Like {@link terminate(int, String)} but uses the given throwable to
   * initialize the ExitException.
   *
   * @param status
   * @param t
   *          throwable used to create the ExitException
   * @throws ExitException
   *           if System.exit is disabled for test purposes
   */
  public static void terminate(int status, Throwable t) throws ExitException {
    terminate(status, StringUtils.stringifyException(t));
  }

  /**
   * Forcibly terminates the currently running Java virtual machine.
   *
   * @param status
   * @param t
   * @throws ExitException
   */
  public static void halt(int status, Throwable t) throws HaltException {
    halt(status, StringUtils.stringifyException(t));
  }

  /**
   * Like {@link terminate(int, String)} without a message.
   *
   * @param status
   * @throws ExitException
   *           if System.exit is disabled for test purposes
   */
  public static void terminate(int status) throws ExitException {
    terminate(status, "ExitException");
  }

  /**
   * Forcibly terminates the currently running Java virtual machine.
   * @param status
   * @throws ExitException
   */
  public static void halt(int status) throws HaltException {
    halt(status, "HaltException");
  }
}
