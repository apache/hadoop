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

package org.apache.slider.server.services.workflow;

import org.junit.Test;

import java.util.concurrent.ExecutorService;


/**
 * Basic tests for executor service
 */
public class TestWorkflowExecutorService extends WorkflowServiceTestBase {

  @Test
  public void testAsyncRun() throws Throwable {

    ExecutorSvc svc = run(new ExecutorSvc());
    ServiceTerminatingRunnable runnable = new ServiceTerminatingRunnable(svc,
        new SimpleRunnable());

    // synchronous in-thread execution
    svc.execute(runnable);
    Thread.sleep(1000);
    assertStopped(svc);
  }

  @Test
  public void testFailureRun() throws Throwable {

    ExecutorSvc svc = run(new ExecutorSvc());
    ServiceTerminatingRunnable runnable = new ServiceTerminatingRunnable(svc,
        new SimpleRunnable(true));

    // synchronous in-thread execution
    svc.execute(runnable);
    Thread.sleep(1000);
    assertStopped(svc);
    assertNotNull(runnable.getException());
  }

  private static class ExecutorSvc
      extends WorkflowExecutorService<ExecutorService> {
    private ExecutorSvc() {
      super("ExecutorService",
          ServiceThreadFactory.singleThreadExecutor("test", true));
    }

  }
}
