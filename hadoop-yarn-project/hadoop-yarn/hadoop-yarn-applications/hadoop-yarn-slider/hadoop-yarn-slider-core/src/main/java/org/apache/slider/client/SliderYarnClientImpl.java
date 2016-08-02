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

package org.apache.slider.client;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.params.ActionNodesArgs;
import org.apache.slider.common.tools.CoreFileSystem;
import org.apache.slider.common.tools.Duration;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class that extends visibility to some of the YarnClientImpl
 * members and data structures, and factors out pure-YARN operations
 * from the slider entry point service
 */
public class SliderYarnClientImpl extends YarnClientImpl {
  protected static final Logger log = LoggerFactory.getLogger(SliderYarnClientImpl.class);

  /**
   * Keyword to use in the {@link #emergencyForceKill(String)}
   * operation to force kill <i>all</i> application instances belonging
   * to a specific user
   */
  public static final String KILL_ALL = "all";

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    InetSocketAddress clientRpcAddress = SliderUtils.getRmAddress(conf);
    if (!SliderUtils.isAddressDefined(clientRpcAddress)) {
      // address isn't known; fail fast
      throw new BindException("Invalid " + YarnConfiguration.RM_ADDRESS
          + " value:" + conf.get(YarnConfiguration.RM_ADDRESS)
          + " - see https://wiki.apache.org/hadoop/UnsetHostnameOrPort");
    }
    super.serviceInit(conf);
  }

  /**
   * Get the RM Client RPC interface
   * @return an RPC interface valid after initialization and authentication
   */
  public ApplicationClientProtocol getRmClient() {
    return rmClient;
  }

  /**
   * List Slider <i>running</i>instances belonging to a specific user.
   * @deprecated use {@link #listDeployedInstances(String)}
   * @param user user: "" means all users
   * @return a possibly empty list of Slider AMs
   */
  public List<ApplicationReport> listInstances(String user)
    throws YarnException, IOException {
    return listDeployedInstances(user);
  }

  /**
   * List Slider <i>deployed</i>instances belonging to a specific user.
   * <p>
   *   Deployed means: known about in the YARN cluster; it will include
   *   any that are in the failed/finished state, as well as those queued
   *   for starting.
   * @param user user: "" means all users
   * @return a possibly empty list of Slider AMs
   */
  public List<ApplicationReport> listDeployedInstances(String user)
    throws YarnException, IOException {
    Preconditions.checkArgument(user != null, "Null User");
    Set<String> types = new HashSet<>(1);
    types.add(SliderKeys.APP_TYPE);
    List<ApplicationReport> allApps = getApplications(types);
    List<ApplicationReport> results = new ArrayList<>();
    for (ApplicationReport report : allApps) {
      if (StringUtils.isEmpty(user) || user.equals(report.getUser())) {
        results.add(report);
      }
    }
    return results;
  }

  /**
   * find all instances of a specific app -if there is more than one in the
   * YARN cluster,
   * this returns them all
   * @param user user; use "" for all users
   * @param appname application name
   * @return the list of all matching application instances
   */
  public List<ApplicationReport> findAllInstances(String user,
                                                  String appname)
      throws IOException, YarnException {
    Preconditions.checkArgument(appname != null, "Null application name");

    List<ApplicationReport> instances = listDeployedInstances(user);
    List<ApplicationReport> results =
      new ArrayList<>(instances.size());
    for (ApplicationReport report : instances) {
      if (report.getName().equals(appname)) {
        results.add(report);
      }
    }
    return results;
  }
  
  /**
   * Helper method to determine if a cluster application is running -or
   * is earlier in the lifecycle
   * @param app application report
   * @return true if the application is considered live
   */
  public boolean isApplicationLive(ApplicationReport app) {
    Preconditions.checkArgument(app != null, "Null app report");

    return app.getYarnApplicationState().ordinal() <= YarnApplicationState.RUNNING.ordinal();
  }


  /**
   * Kill a running application
   * @param applicationId app Id
   * @param reason reason: reason for log
   * @return the response
   * @throws YarnException YARN problems
   * @throws IOException IO problems
   */
  public  KillApplicationResponse killRunningApplication(ApplicationId applicationId,
                                                         String reason)
      throws YarnException, IOException {
    Preconditions.checkArgument(applicationId != null, "Null application Id");
    log.info("Killing application {} - {}", applicationId.getClusterTimestamp(),
             reason);
    KillApplicationRequest request =
      Records.newRecord(KillApplicationRequest.class);
    request.setApplicationId(applicationId);
    return getRmClient().forceKillApplication(request);
  }

  private String getUsername() throws IOException {
    return UserGroupInformation.getCurrentUser().getShortUserName();
  }
  
  /**
   * Force kill a yarn application by ID. No niceties here
   * @param applicationId app Id. "all" means "kill all instances of the current user
   * 
   */
  public void emergencyForceKill(String applicationId)
      throws YarnException, IOException {

    Preconditions.checkArgument(StringUtils.isNotEmpty(applicationId),
        "Null/empty application Id");

    if (KILL_ALL.equals(applicationId)) {
      // user wants all instances killed
      String user = getUsername();
      log.info("Killing all applications belonging to {}", user);
      Collection<ApplicationReport> instances = listDeployedInstances(user);
      for (ApplicationReport instance : instances) {
        if (isApplicationLive(instance)) {
          ApplicationId appId = instance.getApplicationId();
          log.info("Killing Application {}", appId);

          killRunningApplication(appId, "forced kill");
        }
      }
    } else {
      ApplicationId appId = ConverterUtils.toApplicationId(applicationId);

      log.info("Killing Application {}", applicationId);

      killRunningApplication(appId, "forced kill");
    }
  }

  /**
   * Monitor the submitted application for reaching the requested state.
   * Will also report if the app reaches a later state (failed, killed, etc)
   * Kill application if duration!= null & time expires. 
   * @param appId Application Id of application to be monitored
   * @param duration how long to wait -must be more than 0
   * @param desiredState desired state.
   * @return the application report -null on a timeout
   * @throws YarnException
   * @throws IOException
   */
  public ApplicationReport monitorAppToState(
    ApplicationId appId, YarnApplicationState desiredState, Duration duration)
    throws YarnException, IOException {

    if (appId == null) {
      throw new BadCommandArgumentsException("null application ID");
    }
    if (duration.limit <= 0) {
      throw new BadCommandArgumentsException("Invalid monitoring duration");
    }
    log.debug("Waiting {} millis for app to reach state {} ",
              duration.limit,
              desiredState);
    duration.start();
    try {
      while (true) {
        // Get application report for the appId we are interested in

        ApplicationReport r = getApplicationReport(appId);

        log.debug("queried status is\n{}",
          new SliderUtils.OnDemandReportStringifier(r));

        YarnApplicationState state = r.getYarnApplicationState();
        if (state.ordinal() >= desiredState.ordinal()) {
          log.debug("App in desired state (or higher) :{}", state);
          return r;
        }
        if (duration.getLimitExceeded()) {
          log.debug(
            "Wait limit of {} millis to get to state {}, exceeded; app status\n {}",
            duration.limit,
            desiredState,
            new SliderUtils.OnDemandReportStringifier(r));
          return null;
        }

        // sleep 1s.
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
          log.debug("Thread sleep in monitoring loop interrupted");
        }
      }
    } finally {
      duration.close();
    }
  }

  /**
   * find all live instances of a specific app -if there is >1 in the cluster,
   * this returns them all. State should be running or less
   * @param user user
   * @param appname application name
   * @return the list of all matching application instances
   */
  public List<ApplicationReport> findAllLiveInstances(String user,
                                                      String appname) throws
                                                                      YarnException,
                                                                      IOException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(appname),
        "Null/empty application name");
    List<ApplicationReport> instances = listDeployedInstances(user);
    List<ApplicationReport> results =
      new ArrayList<ApplicationReport>(instances.size());
    for (ApplicationReport app : instances) {
      if (app.getName().equals(appname)
          && isApplicationLive(app)) {
        results.add(app);
      }
    }
    return results;
  }

  /**
   * Find a cluster in the instance list; biased towards live instances
   * @param instances list of instances
   * @param appname application name
   * @return the first found instance, else a failed/finished instance, or null
   * if there are none of those
   */
  public ApplicationReport findClusterInInstanceList(List<ApplicationReport> instances,
                                                     String appname) {
    Preconditions.checkArgument(instances != null, "Null instances list");
    Preconditions.checkArgument(StringUtils.isNotEmpty(appname),
        "Null/empty application name");
    // sort by most recent
    SliderUtils.sortApplicationsByMostRecent(instances);
    ApplicationReport found = null;
    for (ApplicationReport app : instances) {
      if (app.getName().equals(appname)) {
        if (isApplicationLive(app)) {
          return app;
        }
        // set the found value if not set
        found = found != null ? found : app;
      }
    }
    return found;
  }

  /**
   * Find an app in the instance list in the desired state 
   * @param instances instance list
   * @param appname application name
   * @param desiredState yarn state desired
   * @return the match or null for none
   */
  public ApplicationReport findAppInInstanceList(List<ApplicationReport> instances,
      String appname,
      YarnApplicationState desiredState) {
    Preconditions.checkArgument(instances != null, "Null instances list");
    Preconditions.checkArgument(StringUtils.isNotEmpty(appname),
        "Null/empty application name");
    Preconditions.checkArgument(desiredState != null, "Null desiredState");
    log.debug("Searching {} records for instance name {} in state '{}'",
        instances.size(), appname, desiredState);
    for (ApplicationReport app : instances) {
      if (app.getName().equals(appname)) {

        YarnApplicationState appstate =
            app.getYarnApplicationState();
        log.debug("app ID {} is in state {}", app.getApplicationId(), appstate);
        if (appstate.equals(desiredState)) {
          log.debug("match");
          return app;
        }
      }
    }
    // nothing found in desired state
    log.debug("No match");
    return null;
  }

  /**
   * List the nodes in the cluster, possibly filtering by node state or label.
   *
   * @param label label to filter by -or "" for any
   * @param live flag to request running nodes only
   * @return a possibly empty list of nodes in the cluster
   * @throws IOException IO problems
   * @throws YarnException YARN problems
   */
  public NodeInformationList listNodes(String label, boolean live)
    throws IOException, YarnException {
    Preconditions.checkArgument(label != null, "null label");
    NodeState[] states;
    if (live) {
      states = new NodeState[1];
      states[0] = NodeState.RUNNING;
    } else {
      states = new NodeState[0];
    }
    List<NodeReport> reports = getNodeReports(states);
    NodeInformationList results = new NodeInformationList(reports.size());
    for (NodeReport report : reports) {
      if (live && report.getNodeState() != NodeState.RUNNING) {
        continue;
      }
      if (!label.isEmpty() && !report.getNodeLabels().contains(label)) {
        continue;
      }
      // build node info from report
      NodeInformation info = new NodeInformation();
      info.hostname = report.getNodeId().getHost();
      info.healthReport  = report.getHealthReport();
      info.httpAddress = report.getHttpAddress();
      info.labels = SliderUtils.extractNodeLabel(report);
      info.rackName = report.getRackName();
      info.state = report.getNodeState().toString();
      results.add(info);
    }
    return results;
  }
}
