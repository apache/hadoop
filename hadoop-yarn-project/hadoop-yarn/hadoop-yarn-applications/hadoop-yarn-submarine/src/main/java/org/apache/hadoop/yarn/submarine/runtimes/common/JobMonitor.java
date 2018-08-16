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

package org.apache.hadoop.yarn.submarine.runtimes.common;

import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.api.JobState;
import org.apache.hadoop.yarn.submarine.common.api.JobStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Monitor status of job(s)
 */
public abstract class JobMonitor {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobMonitor.class);
  protected ClientContext clientContext;

  public JobMonitor(ClientContext clientContext) {
    this.clientContext = clientContext;
  }

  /**
   * Returns status of training job.
   *
   * @param jobName name of job
   * @return job status
   * @throws IOException anything else happens
   * @throws YarnException anything related to YARN happens
   */
  public abstract JobStatus getTrainingJobStatus(String jobName)
      throws IOException, YarnException;

  /**
   * Continue wait and print status if job goes to ready or final state.
   * @param jobName
   * @throws IOException
   * @throws YarnException
   * @throws SubmarineException
   */
  public void waitTrainingFinal(String jobName)
      throws IOException, YarnException, SubmarineException {
    // Wait 5 sec between each fetch.
    int waitIntervalSec = 5;
    JobStatus js;
    while (true) {
      js = getTrainingJobStatus(jobName);
      JobState jobState = js.getState();
      js.nicePrint(System.err);

      if (JobState.isFinal(jobState)) {
        if (jobState.equals(JobState.FAILED)) {
          throw new SubmarineException("Job failed");
        } else if (jobState.equals(JobState.KILLED)) {
          throw new SubmarineException("Job killed");
        }
        LOG.info("Job exited with state=" + jobState);
        break;
      }

      try {
        Thread.sleep(waitIntervalSec * 1000);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }
}
