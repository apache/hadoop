/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.client.cli.runjob;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.submarine.common.api.Role;

/**
 * This class encapsulates data related to a particular Role.
 * Some examples: TF Worker process, TF PS process or a PyTorch worker process.
 */
public class RoleParameters {
  private final Role role;
  private int replicas;
  private String launchCommand;
  private String dockerImage;
  private Resource resource;

  public RoleParameters(Role role, int replicas,
      String launchCommand, String dockerImage, Resource resource) {
    this.role = role;
    this.replicas = replicas;
    this.launchCommand = launchCommand;
    this.dockerImage = dockerImage;
    this.resource = resource;
  }

  public static RoleParameters createEmpty(Role role) {
    return new RoleParameters(role, 0, null, null, null);
  }

  public Role getRole() {
    return role;
  }

  public int getReplicas() {
    return replicas;
  }

  public String getLaunchCommand() {
    return launchCommand;
  }

  public void setLaunchCommand(String launchCommand) {
    this.launchCommand = launchCommand;
  }

  public String getDockerImage() {
    return dockerImage;
  }

  public void setDockerImage(String dockerImage) {
    this.dockerImage = dockerImage;
  }

  public Resource getResource() {
    return resource;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
  }

  public void setReplicas(int replicas) {
    this.replicas = replicas;
  }
}
