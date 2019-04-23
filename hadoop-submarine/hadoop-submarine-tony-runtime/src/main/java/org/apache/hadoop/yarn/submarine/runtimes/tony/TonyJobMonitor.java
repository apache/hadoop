/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.yarn.submarine.runtimes.tony;

import com.linkedin.tony.TonyClient;
import com.linkedin.tony.client.TaskUpdateListener;
import com.linkedin.tony.rpc.TaskInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.api.JobStatus;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.tony.buider.JobStatusBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * An implementation of JobMonitor with TonY library.
 */
public class TonyJobMonitor extends JobMonitor implements TaskUpdateListener {
  private Set<TaskInfo> taskInfos = new HashSet<>();

  public TonyJobMonitor(ClientContext clientContext, TonyClient client) {
    super(clientContext);
    client.addListener(this);
  }

  @Override
  public JobStatus getTrainingJobStatus(String jobName)
      throws IOException, YarnException {
    JobStatus jobStatus = JobStatusBuilder.fromTaskInfoSet(taskInfos);
    jobStatus.setJobName(jobName);
    return jobStatus;
  }

  @Override
  public void onTaskInfosUpdated(Set<TaskInfo> taskInfoSet) {
    this.taskInfos = taskInfoSet;
  }
}
