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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * An implementation of AppChecker that queries the resource manager remotely to
 * determine whether the app is running.
 */
@Private
@Unstable
public class RemoteAppChecker extends AppChecker {

  private static final EnumSet<YarnApplicationState> ACTIVE_STATES = EnumSet
      .of(YarnApplicationState.NEW, YarnApplicationState.ACCEPTED,
          YarnApplicationState.NEW_SAVING, YarnApplicationState.SUBMITTED,
          YarnApplicationState.RUNNING);

  private final YarnClient client;

  public RemoteAppChecker() {
    this(YarnClient.createYarnClient());
  }

  RemoteAppChecker(YarnClient client) {
    super("RemoteAppChecker");
    this.client = client;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    addService(client);
    super.serviceInit(conf);
  }

  @Override
  @Private
  public boolean isApplicationActive(ApplicationId id) throws YarnException {
    ApplicationReport report = null;
    try {
      report = client.getApplicationReport(id);
    } catch (ApplicationNotFoundException e) {
      // the app does not exist
      return false;
    } catch (IOException e) {
      throw new YarnException(e);
    }

    if (report == null) {
      // the app does not exist
      return false;
    }

    return ACTIVE_STATES.contains(report.getYarnApplicationState());
  }

  @Override
  @Private
  public Collection<ApplicationId> getActiveApplications() throws YarnException {
    try {
      List<ApplicationId> activeApps = new ArrayList<ApplicationId>();
      List<ApplicationReport> apps = client.getApplications(ACTIVE_STATES);
      for (ApplicationReport app: apps) {
        activeApps.add(app.getApplicationId());
      }
      return activeApps;
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }
}
