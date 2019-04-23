/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.yarn.submarine.runtimes.tony.buider;

import com.linkedin.tony.rpc.TaskInfo;
import org.apache.hadoop.yarn.submarine.common.api.JobComponentStatus;
import org.apache.hadoop.yarn.submarine.common.api.JobStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JobStatusBuilder builds the job status from a set of TaskInfos.
 */
public final class JobStatusBuilder {
  public static JobStatus fromTaskInfoSet(final Set<TaskInfo> taskInfos) {
    JobStatus status = new JobStatus();
    Set<String> jobNames =
        taskInfos.stream().map(TaskInfo::getName).collect(Collectors.toSet());
    List<JobComponentStatus> jobComponentStatusList = new ArrayList<>();
    for (String jobName : jobNames) {
      Set<TaskInfo> filterTasks = taskInfos.stream()
          .filter(taskInfo -> taskInfo.getName().equals(jobName))
          .collect(Collectors.toSet());
      long numReadyContainers = 0;
      long numRunningContainers = 0;
      long totalAskedContainers = 0;
      for (TaskInfo taskInfo : filterTasks) {
        totalAskedContainers += 1;
        switch (taskInfo.getStatus()) {
        case READY:
          numReadyContainers += 1;
          break;
        case RUNNING:
          numRunningContainers += 1;
          break;
        default:
        }
      }
      jobComponentStatusList.add(new JobComponentStatus(jobName,
          numReadyContainers, numRunningContainers, totalAskedContainers));
    }
    status.setComponentStatus(jobComponentStatusList);
    return status;
  }

  private JobStatusBuilder() { }
}
