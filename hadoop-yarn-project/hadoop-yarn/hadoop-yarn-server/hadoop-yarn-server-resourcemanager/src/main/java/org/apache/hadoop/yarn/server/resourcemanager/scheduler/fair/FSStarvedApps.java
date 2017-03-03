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

import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Helper class to track starved applications.
 *
 * Initially, this uses a blocking queue. We could use other data structures
 * in the future. This class also has some methods to simplify testing.
 */
class FSStarvedApps {

  // List of apps to be processed by the preemption thread.
  private PriorityBlockingQueue<FSAppAttempt> appsToProcess;

  // App being currently processed. This assumes a single reader.
  private FSAppAttempt appBeingProcessed;

  FSStarvedApps() {
    appsToProcess = new PriorityBlockingQueue<>(10, new StarvationComparator());
  }

  /**
   * Add a starved application if it is not already added.
   * @param app application to add
   */
  void addStarvedApp(FSAppAttempt app) {
    if (!app.equals(appBeingProcessed) && !appsToProcess.contains(app)) {
      appsToProcess.add(app);
    }
  }

  /**
   * Blocking call to fetch the next app to process. The returned app is
   * tracked until the next call to this method. This tracking assumes a
   * single reader.
   *
   * @return starved application to process
   * @throws InterruptedException if interrupted while waiting
   */
  FSAppAttempt take() throws InterruptedException {
    // Reset appBeingProcessed before the blocking call
    appBeingProcessed = null;

    // Blocking call to fetch the next starved application
    FSAppAttempt app = appsToProcess.take();
    appBeingProcessed = app;
    return app;
  }

  private static class StarvationComparator implements
      Comparator<FSAppAttempt>, Serializable {
    private static final long serialVersionUID = 1;

    @Override
    public int compare(FSAppAttempt app1, FSAppAttempt app2) {
      int ret = 1;
      if (Resources.fitsIn(app1.getStarvation(), app2.getStarvation())) {
        ret = -1;
      }
      return ret;
    }
  }
}
