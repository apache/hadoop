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

package org.apache.hadoop.service.launcher;

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;

/**
 * This class is intended to be installed by calling 
 * {@link Thread#setDefaultUncaughtExceptionHandler(UncaughtExceptionHandler)}
 * in the main entry point. 
 *
 * The base class will always attempt to shut down the process if an Error
 * was raised; the behavior on a standard Exception, raised outside 
 * process shutdown, is simply to log it. 
 *
 * (Based on the class {@code YarnUncaughtExceptionHandler})
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
@Public
@Evolving
public class HadoopUncaughtExceptionHandler
    implements UncaughtExceptionHandler {

  /**
   * Logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      HadoopUncaughtExceptionHandler.class);

  /**
   * Delegate for simple exceptions.
   */
  private final UncaughtExceptionHandler delegate;

  /**
   * Create an instance delegating to the supplied handler if
   * the exception is considered "simple".
   * @param delegate a delegate exception handler.
   */
  public HadoopUncaughtExceptionHandler(UncaughtExceptionHandler delegate) {
    this.delegate = delegate;
  }

  /**
   * Basic exception handler -logs simple exceptions, then continues.
   */
  public HadoopUncaughtExceptionHandler() {
    this(null);
  }

  /**
   * Uncaught exception handler.
   * If an error is raised: shutdown
   * The state of the system is unknown at this point -attempting
   * a clean shutdown is dangerous. Instead: exit
   * @param thread thread that failed
   * @param exception the raised exception
   */
  @Override
  public void uncaughtException(Thread thread, Throwable exception) {
    if (ShutdownHookManager.get().isShutdownInProgress()) {
      LOG.error("Thread {} threw an error during shutdown: {}.",
          thread.toString(),
          exception,
          exception);
    } else if (exception instanceof Error) {
      try {
        LOG.error("Thread {} threw an error: {}. Shutting down",
            thread.toString(),
            exception,
            exception);
      } catch (Throwable err) {
        // We don't want to not exit because of an issue with logging
      }
      if (exception instanceof OutOfMemoryError) {
        // After catching an OOM java says it is undefined behavior, so don't
        // even try to clean up or we can get stuck on shutdown.
        try {
          System.err.println("Halting due to Out Of Memory Error...");
        } catch (Throwable err) {
          // Again we don't want to exit because of logging issues.
        }
        ExitUtil.haltOnOutOfMemory((OutOfMemoryError) exception);
      } else {
        // error other than OutOfMemory
        ExitUtil.ExitException ee =
            ServiceLauncher.convertToExitException(exception);
        ExitUtil.terminate(ee.status, ee);
      }
    } else {
      // simple exception in a thread. There's a policy decision here:
      // terminate the process vs. keep going after a thread has failed
      // base implementation: do nothing but log
      LOG.error("Thread {} threw an exception: {}",
          thread.toString(),
          exception,
          exception);
      if (delegate != null) {
        delegate.uncaughtException(thread, exception);
      }
    }

  }

}
