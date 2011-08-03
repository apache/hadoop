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

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * Utilities to generate fake test apps
 */
public class MockApps {
  static final Iterator<String> NAMES = Iterators.cycle("SleepJob",
      "RandomWriter", "TeraSort", "TeraGen", "PigLatin", "WordCount",
      "I18nApp<☯>");
  static final Iterator<String> USERS = Iterators.cycle("dorothy", "tinman",
      "scarecrow", "glinda", "nikko", "toto", "winkie", "zeke", "gulch");
  static final Iterator<ApplicationState> STATES = Iterators.cycle(
      ApplicationState.values());
  static final Iterator<String> QUEUES = Iterators.cycle("a.a1", "a.a2",
      "b.b1", "b.b2", "b.b3", "c.c1.c11", "c.c1.c12", "c.c1.c13",
      "c.c2", "c.c3", "c.c4");
  static final long TS = System.currentTimeMillis();

  public static String newAppName() {
    synchronized(NAMES) {
      return NAMES.next();
    }
  }

  public static String newUserName() {
    synchronized(USERS) {
      return USERS.next();
    }
  }

  public static String newQueue() {
    synchronized(QUEUES) {
      return QUEUES.next();
    }
  }

  public static List<ApplicationReport> genApps(int n) {
    List<ApplicationReport> list = Lists.newArrayList();
    for (int i = 0; i < n; ++i) {
      list.add(newApp(i));
    }
    return list;
  }

  public static ApplicationReport newApp(int i) {
    final ApplicationId id = newAppID(i);
    final ApplicationStatus status = newAppStatus();
    final ApplicationState state = newAppState();
    final String user = newUserName();
    final String name = newAppName();
    final String queue = newQueue();
    final Container masterContainer = null;
    return new ApplicationReport() {
      @Override public ApplicationId getApplicationId() { return id; }
      @Override public String getUser() { return user; }
      @Override public String getName() { return name; }
      @Override public ApplicationStatus getStatus() { return status; }
      @Override public ApplicationState getState() { return state; }
      @Override public String getQueue() { return queue; }
      @Override public Container getMasterContainer() {
        return masterContainer;
      }
      @Override public String getTrackingUrl() { return ""; }
      @Override
      public void setApplicationId(ApplicationId applicationId) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public void setMasterContainer(Container container) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public void setTrackingUrl(String url) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public void setName(String name) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public void setQueue(String queue) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public void setState(ApplicationState state) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public void setStatus(ApplicationStatus status) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public void setUser(String user) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public String getDiagnostics() {
        // TODO Auto-generated method stub
        return null;
      }
      @Override
      public void setDiagnostics(String diagnostics) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public String getHost() {
        // TODO Auto-generated method stub
        return null;
      }
      @Override
      public void setHost(String host) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public int getRpcPort() {
        // TODO Auto-generated method stub
        return 0;
      }
      @Override
      public void setRpcPort(int rpcPort) {
        // TODO Auto-generated method stub
        
      }
      @Override
      public String getClientToken() {
        // TODO Auto-generated method stub
        return null;
      }
      @Override
      public void setClientToken(String clientToken) {
        // TODO Auto-generated method stub
        
      }
    };
  }

  public static ApplicationId newAppID(int i) {
    ApplicationId id = Records.newRecord(ApplicationId.class);
    id.setClusterTimestamp(TS);
    id.setId(i);
    return id;
  }

  public static ApplicationAttemptId newAppAttemptID(ApplicationId appId, int i) {
    ApplicationAttemptId id = Records.newRecord(ApplicationAttemptId.class);
    id.setApplicationId(appId);
    id.setAttemptId(i);
    return id;
  }

  public static ApplicationStatus newAppStatus() {
    ApplicationStatus status = Records.newRecord(ApplicationStatus.class);
    status.setProgress((float)Math.random());
    return status;
  }

  public static ApplicationState newAppState() {
    synchronized(STATES) {
      return STATES.next();
    }
  }
}
