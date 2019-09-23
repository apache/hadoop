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

package org.apache.hadoop.yarn.submarine.runtimes.common;

import java.io.IOException;
import java.util.Map;

/**
 * Persistent job/model, etc.
 */
public abstract class SubmarineStorage {
  /**
   * Add a new job by name
   * @param jobName name of job.
   * @param jobInfo info of the job.
   */
  public abstract void addNewJob(String jobName, Map<String, String> jobInfo)
      throws IOException;

  /**
   * Get job info by job name.
   * @param jobName name of job
   * @return info of the job.
   */
  public abstract Map<String, String> getJobInfoByName(String jobName)
      throws IOException;

  /**
   * Add a new model
   * @param modelName name of model
   * @param version version of the model, when null is specified, it will be
   *                "default"
   * @param modelInfo info of the model.
   */
  public abstract void addNewModel(String modelName, String version,
      Map<String, String> modelInfo) throws IOException;

  /**
   * Get model info by name and version.
   *  @param modelName name of model.
   * @param version version of the model, when null is specifed, it will be
   */
  public abstract Map<String, String> getModelInfoByName(String modelName, String version)
      throws IOException;
}
