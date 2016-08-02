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

package org.apache.slider.server.appmaster.state;

import org.apache.hadoop.yarn.api.records.ContainerExitStatus;

/**
 * Container outcomes we care about; slightly simplified from
 * {@link ContainerExitStatus} -and hopefully able to handle
 * any new exit codes.
 */
public enum ContainerOutcome {
  Completed,
  Failed,
  Failed_limits_exceeded,
  Node_failure,
  Preempted;

  /**
   * Build a container outcome from an exit status.
   * The values in {@link ContainerExitStatus} are used
   * here.
   * @param exitStatus exit status
   * @return an enumeration of the outcome.
   */
  public static ContainerOutcome fromExitStatus(int exitStatus) {
    switch (exitStatus) {
      case ContainerExitStatus.ABORTED:
      case ContainerExitStatus.KILLED_BY_APPMASTER:
      case ContainerExitStatus.KILLED_BY_RESOURCEMANAGER:
      case ContainerExitStatus.KILLED_AFTER_APP_COMPLETION:
        // could either be a release or node failure. Treat as completion
        return Completed;
      case ContainerExitStatus.DISKS_FAILED:
        return Node_failure;
      case ContainerExitStatus.PREEMPTED:
        return Preempted;
      case ContainerExitStatus.KILLED_EXCEEDED_PMEM:
      case ContainerExitStatus.KILLED_EXCEEDED_VMEM:
        return Failed_limits_exceeded;
      default:
        return exitStatus == 0 ? Completed : Failed;
    }
  }
}
