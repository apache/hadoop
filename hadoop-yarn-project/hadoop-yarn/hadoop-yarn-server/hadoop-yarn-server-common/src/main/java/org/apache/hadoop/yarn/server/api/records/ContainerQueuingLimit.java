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

package org.apache.hadoop.yarn.server.api.records;

import org.apache.hadoop.yarn.util.Records;

/**
 * Used to hold max wait time / queue length information to be
 * passed back to the NodeManager.
 */
public abstract class ContainerQueuingLimit {

  public static ContainerQueuingLimit newInstance() {
    ContainerQueuingLimit containerQueuingLimit =
        Records.newRecord(ContainerQueuingLimit.class);
    containerQueuingLimit.setMaxQueueLength(-1);
    containerQueuingLimit.setMaxQueueWaitTimeInMs(-1);
    return containerQueuingLimit;
  }

  public abstract int getMaxQueueLength();

  public abstract void setMaxQueueLength(int queueLength);

  public abstract int getMaxQueueWaitTimeInMs();

  public abstract void setMaxQueueWaitTimeInMs(int waitTime);
}
