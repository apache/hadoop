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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * <code>QueuedContainersStatus</code> captures information pertaining to the
 * state of execution of the Queueable containers within a node.
 * </p>
 */
@Private
@Evolving
public abstract class QueuedContainersStatus {
  public static QueuedContainersStatus newInstance() {
    return Records.newRecord(QueuedContainersStatus.class);
  }

  public abstract int getEstimatedQueueWaitTime();

  public abstract void setEstimatedQueueWaitTime(int queueWaitTime);

  public abstract int getWaitQueueLength();

  public abstract void setWaitQueueLength(int waitQueueLength);
}
