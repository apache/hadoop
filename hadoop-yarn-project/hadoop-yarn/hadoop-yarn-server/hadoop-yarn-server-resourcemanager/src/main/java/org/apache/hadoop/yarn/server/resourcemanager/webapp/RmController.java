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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.QUEUE_NAME;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import com.google.inject.Inject;

// Do NOT rename/refactor this to RMView as it will wreak havoc
// on Mac OS HFS as its case-insensitive!
public class RmController extends Controller {

  @Inject
  RmController(RequestContext ctx) {
    super(ctx);
  }

  @Override public void index() {
    setTitle("Applications");
  }

  public void about() {
    setTitle("About the Cluster");
    render(AboutPage.class);
  }

  public void app() {
    render(AppPage.class);
  }

  public void appattempt() {
    render(AppAttemptPage.class);
  }

  public void container() {
    render(ContainerPage.class);
  }

  public void failure() {
    render(RedirectionErrorPage.class);
  }

  public void nodes() {
    render(NodesPage.class);
  }

  public void scheduler() {
    // limit applications to those in states relevant to scheduling
    set(YarnWebParams.APP_STATE, StringHelper.cjoin(
        YarnApplicationState.NEW.toString(),
        YarnApplicationState.NEW_SAVING.toString(),
        YarnApplicationState.SUBMITTED.toString(),
        YarnApplicationState.ACCEPTED.toString(),
        YarnApplicationState.RUNNING.toString()));

    ResourceManager rm = getInstance(ResourceManager.class);
    ResourceScheduler rs = rm.getResourceScheduler();
    if (rs == null || rs instanceof CapacityScheduler) {
      setTitle("Capacity Scheduler");
      render(CapacitySchedulerPage.class);
      return;
    }
    
    if (rs instanceof FairScheduler) {
      setTitle("Fair Scheduler");
      render(FairSchedulerPage.class);
      return;
    }

    if (rs instanceof FifoScheduler) {
      setTitle("FIFO Scheduler");
      render(DefaultSchedulerPage.class);
      return;
    }

    renderOtherPluginScheduler(rm);
  }

  private void renderOtherPluginScheduler(ResourceManager rm) {
    ResourceScheduler rs = rm.getResourceScheduler();
    String schedulerName = rs.getClass().getSimpleName();

    Class<? extends View> cls = PluginSchedulerPageHelper
        .getPageClass(rm.getConfig());
    if (cls != null) {
      setTitle(schedulerName);
      render(cls);
    } else {
      LOG.warn(
          "Render default scheduler page as scheduler page configured doesn't exist");
      setTitle("Default Scheduler");
      render(DefaultSchedulerPage.class);
    }
  }

  static class PluginSchedulerPageHelper {
    private static boolean hasLoaded = false;
    private static Class<? extends View> pageClass = null;
    public static Class<? extends View> getPageClass(Configuration conf) {
      if (!hasLoaded) {
        loadPluginSchedulerPageClass(conf);
        hasLoaded = true;
      }
      return pageClass;
    }

    private static void loadPluginSchedulerPageClass(Configuration conf) {
      Class<?> configuredClass = conf
          .getClass(YarnConfiguration.YARN_HTTP_WEBAPP_SCHEDULER_PAGE, null);
      if (!View.class.isAssignableFrom(configuredClass)) {
        return;
      }
      pageClass = (Class<? extends View>) configuredClass;
    }
  }

  public void queue() {
    setTitle(join("Queue ", get(QUEUE_NAME, "unknown")));
  }

  public void submit() {
    setTitle("Application Submission Not Allowed");
  }
  
  public void nodelabels() {
    setTitle("Node Labels");
    render(NodeLabelsPage.class);
  }

  public void errorsAndWarnings() {
    render(RMErrorsAndWarningsPage.class);
  }

  public void logaggregationstatus() {
    render(AppLogAggregationStatusPage.class);
  }
}
