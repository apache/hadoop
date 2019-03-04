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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * It records an activity operation in allocation,
 * which can be classified as queue, application or container activity.
 * Other information include state, diagnostic, priority.
 */
public class AllocationActivity {
  private String childName = null;
  private String parentName = null;
  private String appPriority = null;
  private String requestPriority = null;
  private ActivityState state;
  private String diagnostic = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationActivity.class);

  public AllocationActivity(String parentName, String queueName,
      String priority, ActivityState state, String diagnostic, String type) {
    this.childName = queueName;
    this.parentName = parentName;
    if (type != null) {
      if (type.equals("app")) {
        this.appPriority = priority;
      } else if (type.equals("container")) {
        this.requestPriority = priority;
      }
    }
    this.state = state;
    this.diagnostic = diagnostic;
  }

  public ActivityNode createTreeNode() {
    if (appPriority != null) {
      return new ActivityNode(this.childName, this.parentName, this.appPriority,
          this.state, this.diagnostic, "app");
    } else if (requestPriority != null) {
      return new ActivityNode(this.childName, this.parentName,
          this.requestPriority, this.state, this.diagnostic, "container");
    } else {
      return new ActivityNode(this.childName, this.parentName, null, this.state,
          this.diagnostic, null);
    }
  }

  public String getName() {
    return this.childName;
  }

  public String getState() {
    return this.state.toString();
  }
}
