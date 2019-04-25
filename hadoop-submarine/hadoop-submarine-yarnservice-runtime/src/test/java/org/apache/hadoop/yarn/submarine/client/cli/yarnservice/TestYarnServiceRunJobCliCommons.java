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

import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.submarine.FileUtilitiesForTests;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceJobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceUtils;

import java.io.IOException;

import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.EXIT_SUCCESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Common operations shared with test classes using Run job-related actions.
 */
public class TestYarnServiceRunJobCliCommons {
  static final String DEFAULT_JOB_NAME = "my-job";
  static final String DEFAULT_DOCKER_IMAGE = "tf-docker:1.1.0";
  static final String DEFAULT_INPUT_PATH = "s3://input";
  static final String DEFAULT_CHECKPOINT_PATH = "s3://output";
  static final String DEFAULT_WORKER_DOCKER_IMAGE = "worker.image";
  static final String DEFAULT_PS_DOCKER_IMAGE = "ps.image";
  static final String DEFAULT_WORKER_LAUNCH_CMD = "python run-job.py";
  static final String DEFAULT_PS_LAUNCH_CMD = "python run-ps.py";
  static final String DEFAULT_TENSORBOARD_RESOURCES = "memory=2G,vcores=2";
  static final String DEFAULT_WORKER_RESOURCES = "memory=2048M,vcores=2";
  static final String DEFAULT_PS_RESOURCES = "memory=4096M,vcores=4";
  static final String DEFAULT_TENSORBOARD_DOCKER_IMAGE = "tb_docker_image:001";

  private FileUtilitiesForTests fileUtils = new FileUtilitiesForTests();

  void setup() throws IOException, YarnException {
    SubmarineLogs.verboseOff();
    AppAdminClient serviceClient = mock(AppAdminClient.class);
    when(serviceClient.actionLaunch(any(String.class), any(String.class),
        any(Long.class), any(String.class))).thenReturn(EXIT_SUCCESS);
    when(serviceClient.getStatusString(any(String.class))).thenReturn(
        "{\"id\": \"application_1234_1\"}");
    YarnServiceUtils.setStubServiceClient(serviceClient);

    fileUtils.setup();
  }

  void teardown() throws IOException {
    fileUtils.teardown();
  }

  FileUtilitiesForTests getFileUtils() {
    return fileUtils;
  }

  Service getServiceSpecFromJobSubmitter(JobSubmitter jobSubmitter) {
    return ((YarnServiceJobSubmitter) jobSubmitter).getServiceWrapper()
        .getService();
  }

}
