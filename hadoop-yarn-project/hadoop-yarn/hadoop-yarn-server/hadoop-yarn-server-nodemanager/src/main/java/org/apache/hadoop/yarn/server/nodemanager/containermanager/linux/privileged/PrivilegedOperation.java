/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents operations that require higher system privileges - e.g
 * creating cgroups, launching containers as specified users, 'tc' commands etc
 * that are completed using the container-executor binary
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrivilegedOperation {

  public enum OperationType {
    CHECK_SETUP("--checksetup"),
    MOUNT_CGROUPS("--mount-cgroups"),
    INITIALIZE_CONTAINER(""), //no CLI switch supported yet
    LAUNCH_CONTAINER(""), //no CLI switch supported yet
    SIGNAL_CONTAINER(""), //no CLI switch supported yet
    DELETE_AS_USER(""), //no CLI switch supported yet
    TC_MODIFY_STATE("--tc-modify-state"),
    TC_READ_STATE("--tc-read-state"),
    TC_READ_STATS("--tc-read-stats"),
    ADD_PID_TO_CGROUP(""); //no CLI switch supported yet.

    private final String option;

    OperationType(String option) {
      this.option = option;
    }

    public String getOption() {
      return option;
    }
  }

  public static final String CGROUP_ARG_PREFIX = "cgroups=";

  private final OperationType opType;
  private final List<String> args;

  public PrivilegedOperation(OperationType opType, String arg) {
    this.opType = opType;
    this.args = new ArrayList<String>();

    if (arg != null) {
      this.args.add(arg);
    }
  }

  public PrivilegedOperation(OperationType opType, List<String> args) {
    this.opType = opType;
    this.args = new ArrayList<String>();

    if (args != null) {
      this.args.addAll(args);
    }
  }

  public void appendArgs(String... args) {
    for (String arg : args) {
      this.args.add(arg);
    }
  }

  public void appendArgs(List<String> args) {
    this.args.addAll(args);
  }

  public OperationType getOperationType() {
    return opType;
  }

  public List<String> getArguments() {
    return Collections.unmodifiableList(this.args);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof PrivilegedOperation)) {
      return false;
    }

    PrivilegedOperation otherOp = (PrivilegedOperation) other;

    return otherOp.opType.equals(opType) && otherOp.args.equals(args);
  }

  @Override
  public int hashCode() {
    return opType.hashCode() + 97 * args.hashCode();
  }
}