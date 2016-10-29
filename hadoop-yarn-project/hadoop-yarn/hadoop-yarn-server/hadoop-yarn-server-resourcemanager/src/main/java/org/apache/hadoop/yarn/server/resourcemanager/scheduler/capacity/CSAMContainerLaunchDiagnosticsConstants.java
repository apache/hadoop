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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

/**
 * diagnostic messages for AMcontainer launching
 */
public interface CSAMContainerLaunchDiagnosticsConstants {
  String SKIP_AM_ALLOCATION_IN_IGNORE_EXCLUSIVE_MODE =
      "Skipping assigning to Node in Ignore Exclusivity mode. ";
  String SKIP_AM_ALLOCATION_IN_BLACK_LISTED_NODE =
      "Skipped scheduling for this Node as its black listed. ";
  String SKIP_AM_ALLOCATION_DUE_TO_LOCALITY =
      "Skipping assigning to Node as request locality is not matching. ";
  String QUEUE_AM_RESOURCE_LIMIT_EXCEED =
      "Queue's AM resource limit exceeded. ";
  String USER_AM_RESOURCE_LIMIT_EXCEED = "User's AM resource limit exceeded. ";
  String LAST_NODE_PROCESSED_MSG =
      " Last Node which was processed for the application : ";
  String CLUSTER_RESOURCE_EMPTY =
      "Skipping AM assignment as cluster resource is empty. ";
}
