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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.HashSet;
import java.util.Set;

public class FairSchedulerWithMockPreemption extends FairScheduler {
  static final long DELAY_FOR_NEXT_STARVATION_CHECK_MS = 10 * 60 * 1000;

  @Override
  protected void createPreemptionThread() {
    preemptionThread = new MockPreemptionThread(this);
  }

  static class MockPreemptionThread extends FSPreemptionThread {
    private Set<FSAppAttempt> appsAdded = new HashSet<>();
    private int totalAppsAdded = 0;

    private MockPreemptionThread(FairScheduler scheduler) {
      super(scheduler);
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        try {
          FSAppAttempt app = context.getStarvedApps().take();
          appsAdded.add(app);
          totalAppsAdded++;
          app.preemptionTriggered(DELAY_FOR_NEXT_STARVATION_CHECK_MS);
        } catch (InterruptedException e) {
          return;
        }
      }
    }

    int uniqueAppsAdded() {
      return appsAdded.size();
    }

    int totalAppsAdded() {
      return totalAppsAdded;
    }
  }
}
