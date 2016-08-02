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

package org.apache.slider.core.registry;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.client.SliderYarnClientImpl;
import org.apache.slider.api.types.SliderInstanceDescription;
import org.apache.slider.common.tools.CoreFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Client code for interacting with a list of service instances.
 * The initial logic just enumerates service instances in the YARN RM
 */
public class YarnAppListClient {

  private final SliderYarnClientImpl yarnClient;
  private final String username;
  private final Configuration conf;
  private static final Logger log =
      LoggerFactory.getLogger(YarnAppListClient.class);

  public YarnAppListClient(SliderYarnClientImpl yarnClient,
      String username,
      Configuration conf) {

    Preconditions.checkArgument(yarnClient != null,
        "yarn client is null: is app inited?");
    Preconditions.checkArgument(username != null,
        "username is null");
    Preconditions.checkArgument(conf != null,
        "conf parameter is null");
    this.yarnClient = yarnClient;
    this.username = username;
    this.conf = conf;
  }

  /**
   * find all live instances of a specific app -if there is more than one 
   * in the cluster, this returns them all. State should be running or earlier
   * in the lifecycle
   * @param appname application name
   * @return the list of all matching application instances
   */
  public List<ApplicationReport> findAllLiveInstances(String appname)
    throws YarnException, IOException {
    return yarnClient.findAllLiveInstances(username, appname);
  }


  /**
   * Find an instance of a application belong to the current user
   * @param appname application name
   * @return the app report or null if none is found
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  public ApplicationReport findInstance(String appname) throws
                                                        YarnException,
                                                        IOException {
    List<ApplicationReport> instances = listInstances(null);
    return yarnClient.findClusterInInstanceList(instances, appname);
  }

  /**
   * List instances belonging to the specific user
   * @return a possibly empty list of AMs
   */
  public List<ApplicationReport> listInstances()
      throws YarnException, IOException {
    return listInstances(null);
  }

  /**
   * List instances belonging to a specific user
   * @return a possibly empty list of AMs
   * @param user user if not the default. null means default, "" means all users, 
   * otherwise it is the name of a user
   */
  public List<ApplicationReport> listInstances(String user)
      throws YarnException, IOException {
    String listUser = user == null ? username : user;
    return yarnClient.listDeployedInstances(listUser);
  }

  /**
   * Enumerate slider instances for the current user, and the
   * most recent app report, where available.
   * @param listOnlyInState boolean to indicate that the instances should
   * only include those in a YARN state
   * <code> minAppState &lt;= currentState &lt;= maxAppState </code>
   * 
   * @param minAppState minimum application state to include in enumeration.
   * @param maxAppState maximum application state to include
   * @return a map of application instance name to description
   * @throws IOException Any IO problem
   * @throws YarnException YARN problems
   */
  public Map<String, SliderInstanceDescription> enumSliderInstances(
      boolean listOnlyInState,
      YarnApplicationState minAppState,
      YarnApplicationState maxAppState)
      throws IOException, YarnException {

    CoreFileSystem sliderFileSystem = new CoreFileSystem(conf);
    Preconditions.checkArgument(!listOnlyInState || minAppState != null,
        "null minAppState when listOnlyInState set");
    Preconditions.checkArgument(!listOnlyInState || maxAppState != null,
        "null maxAppState when listOnlyInState set");
    if (!listOnlyInState) {
      // if there's not filtering, ask for the entire range of states
      minAppState = YarnApplicationState.NEW;
      maxAppState = YarnApplicationState.KILLED;
    }
    // get the complete list of persistent instances
    Map<String, Path> persistentInstances =
        sliderFileSystem.listPersistentInstances();
    Map<String, SliderInstanceDescription> descriptions =
        new HashMap<String, SliderInstanceDescription>(persistentInstances.size());

    if (persistentInstances.isEmpty()) {
      // an empty listing is a success if no cluster was named
      log.debug("No application instances found");
      return descriptions;
    }

    // enum those the RM knows about
    List<ApplicationReport> rmInstances = listInstances();
    SliderUtils.sortApplicationsByMostRecent(rmInstances);
    Map<String, ApplicationReport> reportMap =
        SliderUtils.buildApplicationReportMap(rmInstances, minAppState,
            maxAppState);
    log.debug("Persisted {} deployed {} filtered[{}-{}] & de-duped to {}",
        persistentInstances.size(),
        rmInstances.size(),
        minAppState, maxAppState,
        reportMap.size());

    // at this point there is a list of all persistent instances, and
    // a (possibly filtered) list of application reports

    for (Map.Entry<String, Path> entry : persistentInstances.entrySet()) {
      // loop through the persistent values
      String name = entry.getKey();

      // look up any report from the (possibly filtered) report set
      ApplicationReport report = reportMap.get(name);
      if (!listOnlyInState || report != null) {
        // if the enum wants to filter in state, only add it if there is
        // a report in that range. Otherwise: include all values
        SliderInstanceDescription sid = new SliderInstanceDescription(
            name, entry.getValue(), report);
        descriptions.put(name, sid);
      }
    }

    return descriptions;

  }

}
