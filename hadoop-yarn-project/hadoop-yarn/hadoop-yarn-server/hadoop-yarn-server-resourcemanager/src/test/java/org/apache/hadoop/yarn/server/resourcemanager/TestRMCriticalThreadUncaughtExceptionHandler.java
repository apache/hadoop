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

import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * This class is to test {@link RMCriticalThreadUncaughtExceptionHandler}.
 */
public class TestRMCriticalThreadUncaughtExceptionHandler {
  /**
   * Throw {@link RuntimeException} inside thread and
   * check {@link RMCriticalThreadUncaughtExceptionHandler} instance.
   *
   * Used {@link ExitUtil} class to avoid jvm exit through
   * {@code System.exit(-1)}.
   *
   * @throws InterruptedException if any
   */
  @Test
  public void testUncaughtExceptionHandlerWithError()
      throws InterruptedException {
    ExitUtil.disableSystemExit();

    // Create a MockRM and start it
    ResourceManager resourceManager = new MockRM();
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    resourceManager.getRMContext().getStateStore().start();
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();

    final RMCriticalThreadUncaughtExceptionHandler exHandler =
        new RMCriticalThreadUncaughtExceptionHandler(
            resourceManager.getRMContext());
    final RMCriticalThreadUncaughtExceptionHandler spyRTEHandler =
        spy(exHandler);

    // Create a thread and throw a RTE inside it
    final RuntimeException rte = new RuntimeException("TestRuntimeException");
    final Thread testThread = new Thread(new Runnable() {
      @Override
      public void run() {
        throw rte;
      }
    });
    testThread.setName("TestThread");
    testThread.setUncaughtExceptionHandler(spyRTEHandler);
    assertSame(spyRTEHandler, testThread.getUncaughtExceptionHandler());
    testThread.start();
    testThread.join();

    verify(spyRTEHandler).uncaughtException(testThread, rte);
  }
}