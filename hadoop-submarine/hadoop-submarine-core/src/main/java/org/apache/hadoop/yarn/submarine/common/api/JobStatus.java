/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.common.api;

import java.io.PrintStream;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Status of training job.
 */
public class JobStatus {

  protected String jobName;
  protected JobState state;
  protected String tensorboardLink = "N/A";
  protected List<JobComponentStatus> componentStatus;

  public void nicePrint(PrintStream out) {
    out.println(
        "Job Name=" + this.jobName + ", status=" + state.name() + " time="
            + LocalDateTime.now());
    if (JobState.isFinal(this.state)) {
      return;
    }

    if (tensorboardLink.startsWith("http")) {
      out.println("  Tensorboard link: " + tensorboardLink);
    }

    out.println("  Components:");
    for (JobComponentStatus comp : componentStatus) {
      out.println("    [" + comp.getCompName() + "] Ready=" + comp
          .getNumReadyContainers() + " + Running-But-Non-Ready=" + comp
          .getNumRunningButUnreadyContainers() + " | Asked=" + comp
          .getTotalAskedContainers());
    }
    out.println("------------------");
  }

  public JobState getState() {
    return state;
  }

  public String getTensorboardLink() {
    return tensorboardLink;
  }

  public List<JobComponentStatus> getComponentStatus() {
    return componentStatus;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public void setState(JobState state) {
    this.state = state;
  }

  public void setTensorboardLink(String tensorboardLink) {
    this.tensorboardLink = tensorboardLink;
  }

  public void setComponentStatus(List<JobComponentStatus> componentStatus) {
    this.componentStatus = componentStatus;
  }
}
