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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Facilitates hooking process termination for tests, debugging
 * and embedding.
 * 
 * Hadoop code that attempts to call {@link System#exit(int)} 
 * or {@link Runtime#halt(int)} MUST invoke it via these methods.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce", "YARN"})
@InterfaceStability.Unstable
public final class ExitUtil {
  private static final Logger
      LOG = LoggerFactory.getLogger(ExitUtil.class.getName());
  private static volatile boolean systemExitDisabled = false;
  private static volatile boolean systemHaltDisabled = false;
  private static volatile ExitException firstExitException;
  private static volatile HaltException firstHaltException;
  /** Message raised from an exit exception if none were provided: {@value}. */
  public static final String EXIT_EXCEPTION_MESSAGE = "ExitException";
  /** Message raised from a halt exception if none were provided: {@value}. */
  public static final String HALT_EXCEPTION_MESSAGE = "HaltException";

  private ExitUtil() {
  }

  /**
   * An exception raised when a call to {@link #terminate(int)} was
   * called and system exits were blocked.
   */
  public static class ExitException extends RuntimeException
      implements ExitCodeProvider {
    private static final long serialVersionUID = 1L;
    /**
     * The status code.
     */
    public final int status;

    public ExitException(int status, String msg) {
      super(msg);
      this.status = status;
    }

    public ExitException(int status,
        String message,
        Throwable cause) {
      super(message, cause);
      this.status = status;
    }

    public ExitException(int status, Throwable cause) {
      super(cause);
      this.status = status;
    }

    @Override
    public int getExitCode() {
      return status;
    }

    /**
     * String value does not include exception type, just exit code and message.
     * @return the exit code and any message
     */
    @Override
    public String toString() {
      String message = getMessage();
      if (message == null) {
        message = super.toString();
      }
      return Integer.toString(status) + ": " + message;
    }
  }

  /**
   * An exception raised when a call to {@link #terminate(int)} was
   * called and system halts were blocked.
   */
  public static class HaltException extends RuntimeException
      implements ExitCodeProvider {
    private static final long serialVersionUID = 1L;
    public final int status;

    public HaltException(int status, Throwable cause) {
      super(cause);
      this.status = status;
    }

    public HaltException(int status, String msg) {
      super(msg);
      this.status = status;
    }

    public HaltException(int status,
        String message,
        Throwable cause) {
      super(message, cause);
      this.status = status;
    }

    @Override
    public int getExitCode() {
      return status;
    }

    /**
     * String value does not include exception type, just exit code and message.
     * @return the exit code and any message
     */
    @Override
    public String toString() {
      String message = getMessage();
      if (message == null) {
        message = super.toString();
      }
      return Integer.toString(status) + ": " + message;
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
   * @return true if terminate has been called.
   */
  public static boolean terminateCalled() {
    // Either we set this member or we actually called System#exit
    return firstExitException != null;
  }

  /**
   * @return true if halt has been called.
   */
  public static boolean haltCalled() {
    return firstHaltException != null;
  }

  /**
   * @return the first ExitException thrown, null if none thrown yet.
   */
  public static ExitException getFirstExitException() {
    return firstExitException;
  }

  /**
   * @return the first {@code HaltException} thrown, null if none thrown yet.
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
   * Inner termination: either exit with the exception's exit code,
   * or, if system exits are disabled, rethrow the exception.
   * @param ee exit exception
   */
  public static synchronized void terminate(ExitException ee)
      throws ExitException {
    int status = ee.getExitCode();
    String msg = ee.getMessage();
    if (status != 0) {
      //exit indicates a problem, log it
      LOG.debug("Exiting with status {}: {}",  status, msg, ee);
      LOG.info("Exiting with status {}: {}", status, msg);
    }
    if (systemExitDisabled) {
      LOG.error("Terminate called", ee);
      if (!terminateCalled()) {
        firstExitException = ee;
      }
      throw ee;
    }
    System.exit(status);
  }

  /**
   * Forcibly terminates the currently running Java virtual machine.
   * The exception argument is rethrown if JVM halting is disabled.
   * @param ee the exception containing the status code, message and any stack
   * trace.
   * @throws HaltException if {@link Runtime#halt(int)} is disabled.
   */
  public static synchronized void halt(HaltException ee) throws HaltException {
    int status = ee.getExitCode();
    String msg = ee.getMessage();
    try {
      if (status != 0) {
        //exit indicates a problem, log it
        LOG.debug("Halt with status {}: {}", status, msg, ee);
        LOG.info("Halt with status {}: {}", status, msg, msg);
      }
    } catch (Exception ignored) {
      // ignore exceptions here, as it may be due to an out of memory situation
    }
    if (systemHaltDisabled) {
      LOG.error("Halt called", ee);
      if (!haltCalled()) {
        firstHaltException = ee;
      }
      throw ee;
    }
    Runtime.getRuntime().halt(status);
  }

  /**
   * Like {@link #terminate(int, String)} but uses the given throwable to
   * build the message to display or throw as an
   * {@link ExitException}.
   * <p>
   * @param status exit code to use if the exception is not an ExitException.
   * @param t throwable which triggered the termination. If this exception
   * is an {@link ExitException} its status overrides that passed in.
   * @throws ExitException if {@link System#exit(int)}  is disabled.
   */
  public static void terminate(int status, Throwable t) throws ExitException {
    if (t instanceof ExitException) {
      terminate((ExitException) t);
    } else {
      terminate(new ExitException(status, t));
    }
  }

  /**
   * Forcibly terminates the currently running Java virtual machine.
   *
   * @param status exit code to use if the exception is not a HaltException.
   * @param t throwable which triggered the termination. If this exception
   * is a {@link HaltException} its status overrides that passed in.
   * @throws HaltException if {@link System#exit(int)}  is disabled.
   */
  public static void halt(int status, Throwable t) throws HaltException {
    if (t instanceof HaltException) {
      halt((HaltException) t);
    } else {
      halt(new HaltException(status, t));
    }
  }

  /**
   * Like {@link #terminate(int, Throwable)} without a message.
   *
   * @param status exit code
   * @throws ExitException if {@link System#exit(int)} is disabled.
   */
  public static void terminate(int status) throws ExitException {
    terminate(status, EXIT_EXCEPTION_MESSAGE);
  }

  /**
   * Terminate the current process. Note that terminate is the *only* method
   * that should be used to terminate the daemon processes.
   *
   * @param status exit code
   * @param msg message used to create the {@code ExitException}
   * @throws ExitException if {@link System#exit(int)} is disabled.
   */
  public static void terminate(int status, String msg) throws ExitException {
    terminate(new ExitException(status, msg));
  }

  /**
   * Forcibly terminates the currently running Java virtual machine.
   * @param status status code
   * @throws HaltException if {@link Runtime#halt(int)} is disabled.
   */
  public static void halt(int status) throws HaltException {
    halt(status, HALT_EXCEPTION_MESSAGE);
  }

  /**
   * Forcibly terminates the currently running Java virtual machine.
   * @param status status code
   * @param message message
   * @throws HaltException if {@link Runtime#halt(int)} is disabled.
   */
  public static void halt(int status, String message) throws HaltException {
    halt(new HaltException(status, message));
  }

  /**
   * Handler for out of memory events -no attempt is made here
   * to cleanly shutdown or support halt blocking; a robust
   * printing of the event to stderr is all that can be done.
   * @param oome out of memory event
   */
  public static void haltOnOutOfMemory(OutOfMemoryError oome) {
    //After catching an OOM java says it is undefined behavior, so don't
    //even try to clean up or we can get stuck on shutdown.
    try {
      System.err.println("Halting due to Out Of Memory Error...");
    } catch (Throwable err) {
      //Again we done want to exit because of logging issues.
    }
    Runtime.getRuntime().halt(-1);
  }
}
