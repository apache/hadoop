/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * UpdateApplicationHomeSubClusterResponse contains the answer from the
 * {@code FederationApplicationHomeSubClusterStore} to a request to register the
 * home subcluster of a submitted application. Currently response is empty if
 * the operation was successful, if not an exception reporting reason for a
 * failure.
 */
@Private
@Unstable
public abstract class UpdateApplicationHomeSubClusterResponse {

  @Private
  @Unstable
  public static UpdateApplicationHomeSubClusterResponse newInstance() {
    UpdateApplicationHomeSubClusterResponse response =
        Records.newRecord(UpdateApplicationHomeSubClusterResponse.class);
    return response;
  }

}
