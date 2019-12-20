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

package org.apache.hadoop.yarn.submarine.client.cli.param.yaml;

import java.util.List;
import java.util.Map;

/**
 * Base class for Roles. 'roles' is a section of the YAML configuration file.
 */
public class Role {
  private String resources;
  private int replicas;
  private String launchCmd;

  //Optional parameters (Can override global config)
  private String dockerImage;
  private Map<String, String> envs;
  private List<String> localizations;
  private List<String> mounts;

  public String getResources() {
    return resources;
  }

  public void setResources(String resources) {
    this.resources = resources;
  }

  public int getReplicas() {
    return replicas;
  }

  public void setReplicas(int replicas) {
    this.replicas = replicas;
  }

  public String getLaunchCmd() {
    return launchCmd;
  }

  public void setLaunchCmd(String launchCmd) {
    this.launchCmd = launchCmd;
  }

  public String getDockerImage() {
    return dockerImage;
  }

  public void setDockerImage(String dockerImage) {
    this.dockerImage = dockerImage;
  }

  public Map<String, String> getEnvs() {
    return envs;
  }

  public void setEnvs(Map<String, String> envs) {
    this.envs = envs;
  }

  public List<String> getLocalizations() {
    return localizations;
  }

  public void setLocalizations(List<String> localizations) {
    this.localizations = localizations;
  }

  public List<String> getMounts() {
    return mounts;
  }

  public void setMounts(List<String> mounts) {
    this.mounts = mounts;
  }
}
