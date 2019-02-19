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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.runtimes.RuntimeFactory;
import org.apache.hadoop.yarn.submarine.runtimes.common.FSBasedSubmarineStorageImpl;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;

public class YarnServiceRuntimeFactory extends RuntimeFactory {

  public YarnServiceRuntimeFactory(ClientContext clientContext) {
    super(clientContext);
  }

  @Override
  protected JobSubmitter internalCreateJobSubmitter() {
    return new YarnServiceJobSubmitter(super.clientContext);
  }

  @Override
  protected JobMonitor internalCreateJobMonitor() {
    return new YarnServiceJobMonitor(super.clientContext);
  }

  @Override
  protected SubmarineStorage internalCreateSubmarineStorage() {
    return new FSBasedSubmarineStorageImpl(super.clientContext);
  }
}
