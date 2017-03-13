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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.conf.HAUtil;

/**
 * This class either shuts down {@link ResourceManager} or transitions the
 * {@link ResourceManager} to standby state if a critical thread throws an
 * uncaught exception. It is intended to be installed by calling
 * {@code setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)}
 * in the thread entry point or after creation of threads.
 */
@Private
public class RMCriticalThreadUncaughtExceptionHandler
    implements UncaughtExceptionHandler {
  private static final Log LOG = LogFactory.getLog(
      RMCriticalThreadUncaughtExceptionHandler.class);
  private final RMContext rmContext;

  public RMCriticalThreadUncaughtExceptionHandler(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    LOG.fatal("Critical thread " + t.getName() + " crashed!", e);

    if (HAUtil.isHAEnabled(rmContext.getYarnConfiguration())) {
      rmContext.getResourceManager().handleTransitionToStandByInNewThread();
    } else {
      rmContext.getDispatcher().getEventHandler().handle(
          new RMFatalEvent(RMFatalEventType.CRITICAL_THREAD_CRASH,
              new Exception(e)));
    }
  }
}
