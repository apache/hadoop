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
 * Class that holds values found in 'configs' section of YAML configuration.
 */
public class Configs {
  private String dockerImage;
  private String inputPath;
  private String savedModelPath;
  private String checkpointPath;
  private List<String> quicklinks;
  private String waitJobFinish;
  private Map<String, String> envs;
  private List<String> localizations;
  private List<String> mounts;

  public String getDockerImage() {
    return dockerImage;
  }

  public void setDockerImage(String dockerImage) {
    this.dockerImage = dockerImage;
  }

  public String getInputPath() {
    return inputPath;
  }

  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  public String getSavedModelPath() {
    return savedModelPath;
  }

  public void setSavedModelPath(String savedModelPath) {
    this.savedModelPath = savedModelPath;
  }

  public String getCheckpointPath() {
    return checkpointPath;
  }

  public void setCheckpointPath(String checkpointPath) {
    this.checkpointPath = checkpointPath;
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

  public List<String> getQuicklinks() {
    return quicklinks;
  }

  public void setQuicklinks(List<String> quicklinks) {
    this.quicklinks = quicklinks;
  }

  public String getWaitJobFinish() {
    return waitJobFinish;
  }

  public void setWaitJobFinish(String waitJobFinish) {
    this.waitJobFinish = waitJobFinish;
  }
}
