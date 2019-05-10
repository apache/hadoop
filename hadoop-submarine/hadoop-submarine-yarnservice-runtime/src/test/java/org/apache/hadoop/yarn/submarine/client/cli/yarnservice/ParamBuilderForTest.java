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

package org.apache.hadoop.yarn.submarine.client.cli.yarnservice;

import com.google.common.collect.Lists;

import java.util.List;

class ParamBuilderForTest {
  private final List<String> params = Lists.newArrayList();

  static ParamBuilderForTest create() {
    return new ParamBuilderForTest();
  }

  ParamBuilderForTest withJobName(String name) {
    params.add("--name");
    params.add(name);
    return this;
  }

  ParamBuilderForTest withFramework(String framework) {
    params.add("--framework");
    params.add(framework);
    return this;
  }

  ParamBuilderForTest withDockerImage(String dockerImage) {
    params.add("--docker_image");
    params.add(dockerImage);
    return this;
  }

  ParamBuilderForTest withInputPath(String inputPath) {
    params.add("--input_path");
    params.add(inputPath);
    return this;
  }

  ParamBuilderForTest withCheckpointPath(String checkpointPath) {
    params.add("--checkpoint_path");
    params.add(checkpointPath);
    return this;
  }

  ParamBuilderForTest withNumberOfWorkers(int numWorkers) {
    params.add("--num_workers");
    params.add(String.valueOf(numWorkers));
    return this;
  }

  ParamBuilderForTest withNumberOfPs(int numPs) {
    params.add("--num_ps");
    params.add(String.valueOf(numPs));
    return this;
  }

  ParamBuilderForTest withWorkerLaunchCommand(String launchCommand) {
    params.add("--worker_launch_cmd");
    params.add(launchCommand);
    return this;
  }

  ParamBuilderForTest withPsLaunchCommand(String launchCommand) {
    params.add("--ps_launch_cmd");
    params.add(launchCommand);
    return this;
  }

  ParamBuilderForTest withWorkerResources(String workerResources) {
    params.add("--worker_resources");
    params.add(workerResources);
    return this;
  }

  ParamBuilderForTest withPsResources(String psResources) {
    params.add("--ps_resources");
    params.add(psResources);
    return this;
  }

  ParamBuilderForTest withWorkerDockerImage(String dockerImage) {
    params.add("--worker_docker_image");
    params.add(dockerImage);
    return this;
  }

  ParamBuilderForTest withPsDockerImage(String dockerImage) {
    params.add("--ps_docker_image");
    params.add(dockerImage);
    return this;
  }

  ParamBuilderForTest withVerbose() {
    params.add("--verbose");
    return this;
  }

  ParamBuilderForTest withTensorboard() {
    params.add("--tensorboard");
    return this;
  }

  ParamBuilderForTest withTensorboardResources(String resources) {
    params.add("--tensorboard_resources");
    params.add(resources);
    return this;
  }

  ParamBuilderForTest withTensorboardDockerImage(String dockerImage) {
    params.add("--tensorboard_docker_image");
    params.add(dockerImage);
    return this;
  }

  ParamBuilderForTest withQuickLink(String quickLink) {
    params.add("--quicklink");
    params.add(quickLink);
    return this;
  }

  ParamBuilderForTest withLocalization(String remoteUrl, String localUrl) {
    params.add("--localization");
    params.add(remoteUrl + ":" + localUrl);
    return this;
  }

  String[] build() {
    return params.toArray(new String[0]);
  }
}
