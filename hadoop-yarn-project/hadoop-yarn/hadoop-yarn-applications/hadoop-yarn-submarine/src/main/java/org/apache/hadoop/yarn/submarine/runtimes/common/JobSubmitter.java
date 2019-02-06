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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;

import java.io.IOException;

/**
 * Submit job to cluster master
 */
public interface JobSubmitter {
  /**
   * Submit job to cluster
   * @param parameters run job parameters
   * @return applicatioId when successfully submitted
   * @throws YarnException for issues while contacting YARN daemons
   * @throws IOException for other issues.
   */
  ApplicationId submitJob(RunJobParameters parameters)
      throws IOException, YarnException;
}
