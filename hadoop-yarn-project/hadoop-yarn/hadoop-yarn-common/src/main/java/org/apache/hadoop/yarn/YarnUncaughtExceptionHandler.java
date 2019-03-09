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

package org.apache.hadoop.yarn;

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;

/**
 * This class is intended to be installed by calling 
 * {@link Thread#setDefaultUncaughtExceptionHandler(UncaughtExceptionHandler)}
 * In the main entry point.  It is intended to try and cleanly shut down
 * programs using the YARN Event framework.
 * 
 * Note: Right now it only will shut down the program if a Error is caught, but
 * not any other exception.  Anything else is just logged.
 */
@Public
@Evolving
public class YarnUncaughtExceptionHandler implements UncaughtExceptionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(YarnUncaughtExceptionHandler.class);
  private static final Marker FATAL =
      MarkerFactory.getMarker("FATAL");
  
  @Override
  public void uncaughtException(Thread t, Throwable e) {
    if(ShutdownHookManager.get().isShutdownInProgress()) {
      LOG.error("Thread " + t + " threw an Throwable, but we are shutting " +
      		"down, so ignoring this", e);
    } else if(e instanceof Error) {
      try {
        LOG.error(FATAL,
            "Thread " + t + " threw an Error.  Shutting down now...", e);
      } catch (Throwable err) {
        //We don't want to not exit because of an issue with logging
      }
      if(e instanceof OutOfMemoryError) {
        //After catching an OOM java says it is undefined behavior, so don't
        //even try to clean up or we can get stuck on shutdown.
        try {
          System.err.println("Halting due to Out Of Memory Error...");
        } catch (Throwable err) {
          //Again we done want to exit because of logging issues.
        }
        ExitUtil.halt(-1);
      } else {
        ExitUtil.terminate(-1);
      }
    } else {
      LOG.error("Thread " + t + " threw an Exception.", e);
    }
  }
}
